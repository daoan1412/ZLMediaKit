/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include <atomic>
#include <iomanip>
#include "Common/config.h"
#include "UDPServer.h"
#include "RtspSession.h"
#include "Util/MD5.h"
#include "Util/base64.h"
#include "RtpMultiCaster.h"
#include "Rtcp/RtcpContext.h"

using namespace std;
using namespace toolkit;

namespace mediakit {

/**
 * RTSP protocol has multiple ways to transmit RTP data packets, currently supporting the following 4 methods:
 * 1: rtp over udp - RTP is transmitted through separate UDP ports
 * 2: rtp over udp_multicast - RTP is transmitted through shared UDP multicast ports
 * 3: rtp over tcp - Transmission is completed through RTSP signaling TCP channels
 * 4: rtp over http - The following explains rtp over http in detail:
 *
 * rtp over http disguises the RTSP protocol as HTTP protocol to penetrate firewalls.
 * At this time, the player will send two HTTP requests to the RTSP server: the first is an HTTP GET request,
 * and the second is an HTTP POST request.
 *
 * These two requests are bound together through the x-sessioncookie key in the HTTP request headers.
 *
 * The first HTTP GET request is used to receive RTP, RTCP and RTSP replies, and no other requests are sent on this connection afterwards.
 * The second HTTP POST request is used to send RTSP requests. After the RTSP handshake ends, this connection may be disconnected, but we still need to maintain RTP transmission.
 * It should be noted that the content payload in the HTTP POST request is the base64-encoded RTSP request packet.
 * The player disguises RTSP requests as HTTP content payload and sends them to the RTSP server, then the RTSP server sends replies to the TCP connection of the first HTTP GET request.
 * This way, from the firewall's perspective, this RTSP session is just two HTTP requests, and the firewall will allow the data to pass through.
 *
 * When ZLMediaKit processes RTSP over HTTP requests, it base64-decodes the content data from the HTTP poster and forwards it to the HTTP getter for processing.
 */


// HTTP GET request instance for RTSP over HTTP, used to receive RTP packets
static unordered_map<string, weak_ptr<RtspSession> > g_mapGetter;
// Mutex protection for g_mapGetter
static recursive_mutex g_mtxGetter;

RtspSession::RtspSession(const Socket::Ptr &sock) : Session(sock) {
    GET_CONFIG(uint32_t,keep_alive_sec,Rtsp::kKeepAliveSecond);
    sock->setSendTimeOutSecond(keep_alive_sec);
}

void RtspSession::onError(const SockException &err) {
    bool is_player = !_push_src_ownership;
    uint64_t duration = _alive_ticker.createdTime() / 1000;
    WarnP(this) << (is_player ? "RTSP player(" : "RTSP pusher(")
                << _media_info.shortUrl()
                << ") disconnected:" << err.what()
                << ", duration(s):" << duration;

    if (_rtp_type == Rtsp::RTP_MULTICAST) {
        // Cancel UDP port listening
        UDPServer::Instance().stopListenPeer(get_peer_ip().data(), this);
    }

    if (_http_x_sessioncookie.size() != 0) {
        // Remove weak reference record of http getter
        lock_guard<recursive_mutex> lock(g_mtxGetter);
        g_mapGetter.erase(_http_x_sessioncookie);
    }

    // Traffic statistics event broadcast
    GET_CONFIG(uint32_t, iFlowThreshold, General::kFlowThreshold);
    if (_bytes_usage >= iFlowThreshold * 1024) {
        NOTICE_EMIT(BroadcastFlowReportArgs, Broadcast::kBroadcastFlowReport, _media_info, _bytes_usage, duration, is_player, *this);
    }

    // If actively closed, do not delay unregistration
    if (_push_src && _continue_push_ms  && err.getErrCode() != Err_shutdown) {
        // Cancel ownership
        _push_src_ownership = nullptr;
        // Delay stream unregistration
        auto push_src = std::move(_push_src);
        getPoller()->doDelayTask(_continue_push_ms, [push_src]() { return 0; });
    }
}

void RtspSession::onManager() {
    GET_CONFIG(uint32_t, handshake_sec, Rtsp::kHandshakeSecond);
    GET_CONFIG(uint32_t, keep_alive_sec, Rtsp::kKeepAliveSecond);

    if (_alive_ticker.createdTime() > handshake_sec * 1000) {
        if (_sessionid.size() == 0) {
            shutdown(SockException(Err_timeout,"illegal connection"));
            return;
        }
    }

    if (_push_src && _alive_ticker.elapsedTime() > keep_alive_sec * 1000) {
        //推流超时
        shutdown(SockException(Err_timeout, "pusher session timeout"));
        return;
    }

    if (!_push_src && _rtp_type == Rtsp::RTP_UDP && _alive_ticker.elapsedTime() > keep_alive_sec * 4000) {
        // RTP over UDP player timeout
        shutdown(SockException(Err_timeout, "rtp over udp player timeout"));
    }
}

void RtspSession::onRecv(const Buffer::Ptr &buf) {
    _alive_ticker.resetTime();
    _bytes_usage += buf->size();
    if (_on_recv) {
        // HTTP poster request data forwarded to HTTP getter for processing
        _on_recv(buf);
    } else {
        input(buf->data(), buf->size());
    }
}

void RtspSession::onWholeRtspPacket(Parser &parser) {
    string method = parser.method(); // Extract request command
    _cseq = atoi(parser["CSeq"].data());
    if (_content_base.empty() && method != "GET" && method != "POST" ) {
        RtspUrl rtsp;
        rtsp.parse(parser.url());
        _content_base = rtsp._url;
        _media_info.parse(parser.fullUrl());
        _media_info.schema = RTSP_SCHEMA;
        _media_info.protocol = overSsl() ? "rtsps" : "rtsp";
    }

    using rtsp_request_handler = void (RtspSession::*)(const Parser &parser);
    static unordered_map<string, rtsp_request_handler> s_cmd_functions;
    static onceToken token([]() {
        s_cmd_functions.emplace("OPTIONS", &RtspSession::handleReq_Options);
        s_cmd_functions.emplace("DESCRIBE", &RtspSession::handleReq_Describe);
        s_cmd_functions.emplace("ANNOUNCE", &RtspSession::handleReq_ANNOUNCE);
        s_cmd_functions.emplace("RECORD", &RtspSession::handleReq_RECORD);
        s_cmd_functions.emplace("SETUP", &RtspSession::handleReq_Setup);
        s_cmd_functions.emplace("PLAY", &RtspSession::handleReq_Play);
        s_cmd_functions.emplace("PAUSE", &RtspSession::handleReq_Pause);
        s_cmd_functions.emplace("TEARDOWN", &RtspSession::handleReq_Teardown);
        s_cmd_functions.emplace("GET", &RtspSession::handleReq_Get);
        s_cmd_functions.emplace("POST", &RtspSession::handleReq_Post);
        s_cmd_functions.emplace("SET_PARAMETER", &RtspSession::handleReq_SET_PARAMETER);
        s_cmd_functions.emplace("GET_PARAMETER", &RtspSession::handleReq_SET_PARAMETER);
    });

    auto it = s_cmd_functions.find(method);
    if (it == s_cmd_functions.end()) {
        sendRtspResponse("403 Forbidden");
        throw SockException(Err_shutdown, StrPrinter << "403 Forbidden:" << method);
    }

    (this->*(it->second))(parser);
    parser.clear();
}

void RtspSession::onRtpPacket(const char *data, size_t len) {
    uint8_t interleaved = data[1];
    if (interleaved % 2 == 0) {
        CHECK(len > RtpPacket::kRtpHeaderSize + RtpPacket::kRtpTcpHeaderSize);
        RtpHeader *header = (RtpHeader *)(data + RtpPacket::kRtpTcpHeaderSize);
        auto track_idx = getTrackIndexByPT(header->pt);
        handleOneRtp(track_idx, _sdp_track[track_idx]->_type, _sdp_track[track_idx]->_samplerate, (uint8_t *) data + RtpPacket::kRtpTcpHeaderSize, len - RtpPacket::kRtpTcpHeaderSize);
    } else {
        auto track_idx = getTrackIndexByInterleaved(interleaved - 1);
        onRtcpPacket(track_idx, _sdp_track[track_idx], data + RtpPacket::kRtpTcpHeaderSize, len - RtpPacket::kRtpTcpHeaderSize);
    }
}

void RtspSession::onRtcpPacket(int track_idx, SdpTrack::Ptr &track, const char *data, size_t len){
    auto rtcp_arr = RtcpHeader::loadFromBytes((char *) data, len);
    for (auto &rtcp : rtcp_arr) {
        _rtcp_context[track_idx]->onRtcp(rtcp);
        if ((RtcpType) rtcp->pt == RtcpType::RTCP_SR) {
            auto sr = (RtcpSR *) (rtcp);
            // Set correspondence between RTP timestamp and NTP timestamp
            setNtpStamp(track_idx, sr->rtpts, sr->getNtpUnixStampMS());
        }
    }
}

ssize_t RtspSession::getContentLength(Parser &parser) {
    if(parser.method() == "POST"){
        // The content data part of HTTP POST request is base64-encoded RTSP request signaling packet
        return remainDataSize();
    }
    return RtspSplitter::getContentLength(parser);
}

void RtspSession::handleReq_Options(const Parser &parser) {
    // Support these commands
    sendRtspResponse("200 OK",{"Public" , "OPTIONS, DESCRIBE, SETUP, TEARDOWN, PLAY, PAUSE, ANNOUNCE, RECORD, SET_PARAMETER, GET_PARAMETER"});
}

void RtspSession::handleReq_ANNOUNCE(const Parser &parser) {
    auto full_url = parser.fullUrl();
    _content_base = full_url;
    if (end_with(full_url, ".sdp")) {
        // Remove .sdp suffix to prevent EasyDarwin pusher from forcibly adding .sdp suffix
        full_url = full_url.substr(0, full_url.length() - 4);
        _media_info.parse(full_url);
        _media_info.protocol = overSsl() ? "rtsps" : "rtsp";
    }

    if (_media_info.app.empty() || _media_info.stream.empty()) {
        // RTSP push URL must have at least two levels (rtsp://host/app/stream_id), inexplicable push URLs are not allowed
        static constexpr auto err = "Illegal RTSP push URL, ensure at least two-level RTSP URL";
        sendRtspResponse("403 Forbidden", {"Content-Type", "text/plain"}, err);
        throw SockException(Err_shutdown, StrPrinter << err << ":" << full_url);
    }

    auto onRes = [this, parser, full_url](const string &err, const ProtocolOption &option) {
        if (!err.empty()) {
            sendRtspResponse("401 Unauthorized", { "Content-Type", "text/plain" }, err);
            shutdown(SockException(Err_shutdown, StrPrinter << "401 Unauthorized:" << err));
            return;
        }

        assert(!_push_src);
        auto src = MediaSource::find(RTSP_SCHEMA, _media_info.vhost, _media_info.app, _media_info.stream);
        auto push_failed = (bool)src;

        while (src) {
            // Try to continue pushing after disconnection
            auto rtsp_src = dynamic_pointer_cast<RtspMediaSourceImp>(src);
            if (!rtsp_src) {
                // Source is not generated by RTSP push
                break;
            }
            auto ownership = rtsp_src->getOwnership();
            if (!ownership) {
                // Failed to get push source ownership
                break;
            }
            _push_src = std::move(rtsp_src);
            _push_src_ownership = std::move(ownership);
            push_failed = false;
            break;
        }

        if (push_failed) {
            sendRtspResponse("406 Not Acceptable", { "Content-Type", "text/plain" }, "Already publishing.");
            string err = StrPrinter << "ANNOUNCE: Already publishing:" << _media_info.shortUrl() << endl;
            throw SockException(Err_shutdown, err);
        }

        SdpParser sdpParser(parser.content());
        _sessionid = makeRandStr(12);
        _sdp_track = sdpParser.getAvailableTrack();
        if (_sdp_track.empty()) {
            // Invalid SDP
            static constexpr auto err = "No valid track in SDP";
            sendRtspResponse("403 Forbidden", { "Content-Type", "text/plain" }, err);
            shutdown(SockException(Err_shutdown, StrPrinter << err << ":" << full_url));
            return;
        }
        _rtcp_context.clear();
        for (auto &track : _sdp_track) {
            _rtcp_context.emplace_back(std::make_shared<RtcpContextForRecv>());
        }

        if (!_push_src) {
            _push_src = std::make_shared<RtspMediaSourceImp>(_media_info);
            // Get ownership
            _push_src_ownership = _push_src->getOwnership();
            _push_src->setProtocolOption(option);
            _push_src->setSdp(parser.content());
        }

        _push_src->setListener(static_pointer_cast<RtspSession>(shared_from_this()));
        _continue_push_ms = option.continue_push_ms;
        sendRtspResponse("200 OK");
    };

    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    Broadcast::PublishAuthInvoker invoker = [weak_self, onRes](const string &err, const ProtocolOption &option) {
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            return;
        }
        strong_self->async([weak_self, onRes, err, option]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                return;
            }
            onRes(err, option);
        });
    };

    // RTSP push requires authentication
    auto flag = NOTICE_EMIT(BroadcastMediaPublishArgs, Broadcast::kBroadcastMediaPublish, MediaOriginType::rtsp_push, _media_info, invoker, *this);
    if (!flag) {
        // No one is listening to this event, default no authentication
        onRes("", ProtocolOption());
    }
}

void RtspSession::handleReq_RECORD(const Parser &parser){
    if (_sdp_track.empty() || parser["Session"] != _sessionid) {
        send_SessionNotFound();
        throw SockException(Err_shutdown, _sdp_track.empty() ? "can not find any available track when record" : "session not found when record");
    }

    _StrPrinter rtp_info;
    for (auto &track : _sdp_track) {
        if (track->_inited == false) {
            // There are still tracks that haven't been set up
            shutdown(SockException(Err_shutdown, "track not setuped"));
            return;
        }
        rtp_info << "url=" << track->getControlUrl(_content_base) << ",";
    }
    rtp_info.pop_back();
    sendRtspResponse("200 OK", {"RTP-Info", rtp_info});
    if (_rtp_type == Rtsp::RTP_TCP) {
        // If it's an RTSP push server and TCP push, set socket flags to improve receiving performance
        setSocketFlags();
    }
}

void RtspSession::emitOnPlay(){
    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    // URL authentication callback
    auto onRes = [weak_self](const string &err) {
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            return;
        }
        if (!err.empty()) {
            // Play URL authentication failed
            strong_self->sendRtspResponse("401 Unauthorized", {"Content-Type", "text/plain"}, err);
            strong_self->shutdown(SockException(Err_shutdown, StrPrinter << "401 Unauthorized:" << err));
            return;
        }
        strong_self->onAuthSuccess();
    };

    Broadcast::AuthInvoker invoker = [weak_self, onRes](const string &err) {
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            return;
        }
        strong_self->async([onRes, err, weak_self]() {
            onRes(err);
        });
    };

    // Broadcast general play URL authentication event
    auto flag = _emit_on_play ? false : NOTICE_EMIT(BroadcastMediaPlayedArgs, Broadcast::kBroadcastMediaPlayed, _media_info, invoker, *this);
    if (!flag) {
        // No one is listening to this event, default no authentication
        onRes("");
    }
    // Already authenticated
    _emit_on_play = true;
}

void RtspSession::handleReq_Describe(const Parser &parser) {
    // Authentication information in this request
    auto authorization = parser["Authorization"];
    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    // RTSP-specific authentication event callback
    onGetRealm invoker = [weak_self, authorization](const string &realm) {
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            // This object has been destroyed
            return;
        }
        // Switch to own thread and execute
        strong_self->async([weak_self, realm, authorization]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                // This object has been destroyed
                return;
            }
            if (realm.empty()) {
                // No RTSP-specific authentication needed, continue with general URL authentication (on_play)
                strong_self->emitOnPlay();
                return;
            }
            // This stream requires RTSP-specific authentication, after enabling RTSP-specific authentication, general URL authentication (on_play) will no longer be triggered
            strong_self->_rtsp_realm = realm;
            strong_self->onAuthUser(realm, authorization);
        });
    };

    if(_rtsp_realm.empty()){
        // Broadcast whether RTSP-specific authentication is needed
        if (!NOTICE_EMIT(BroadcastOnGetRtspRealmArgs, Broadcast::kBroadcastOnGetRtspRealm, _media_info, invoker, *this)) {
            // No one is listening to this event, indicating no authentication is needed
            invoker("");
        }
    }else{
        invoker(_rtsp_realm);
    }
}

void RtspSession::onAuthSuccess() {
    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    MediaSource::findAsync(_media_info, weak_self.lock(), [weak_self](const MediaSource::Ptr &src){
        auto strong_self = weak_self.lock();
        if(!strong_self){
            return;
        }
        auto rtsp_src = dynamic_pointer_cast<RtspMediaSource>(src);
        if (!rtsp_src) {
            // Corresponding MediaSource not found
            string err = StrPrinter << "no such stream:" << strong_self->_media_info.shortUrl();
            strong_self->send_StreamNotFound();
            strong_self->shutdown(SockException(Err_shutdown,err));
            return;
        }
        // Found the corresponding RTSP stream
        strong_self->_sdp_track = SdpParser(rtsp_src->getSdp()).getAvailableTrack();
        if (strong_self->_sdp_track.empty()) {
            // This stream is invalid
            WarnL << "No valid track in SDP, this stream is invalid:" << rtsp_src->getSdp();
            strong_self->send_StreamNotFound();
            strong_self->shutdown(SockException(Err_shutdown,"can not find any available track in sdp"));
            return;
        }
        strong_self->_rtcp_context.clear();
        for (auto &track : strong_self->_sdp_track) {
            strong_self->_rtcp_context.emplace_back(std::make_shared<RtcpContextForSend>());
        }
        strong_self->_sessionid = makeRandStr(12);
        strong_self->_play_src = rtsp_src;
        for(auto &track : strong_self->_sdp_track){
            track->_ssrc = rtsp_src->getSsrc(track->_type);
            track->_seq = rtsp_src->getSeqence(track->_type);
            track->_time_stamp = rtsp_src->getTimeStamp(track->_type);
        }

        strong_self->sendRtspResponse("200 OK",
                                     {"Content-Base", strong_self->_content_base + "/",
                                      "x-Accept-Retransmit","our-retransmit",
                                      "x-Accept-Dynamic-Rate","1"
                                     },rtsp_src->getSdp());
    });
}

void RtspSession::onAuthFailed(const string &realm,const string &why,bool close) {
    GET_CONFIG(bool, authBasic, Rtsp::kAuthBasic);
    if (!authBasic) {
        // We need the client to prioritize md5 authentication
        _auth_nonce = makeRandStr(32);
        sendRtspResponse("401 Unauthorized", { "WWW-Authenticate", StrPrinter << "Digest realm=\"" << realm << "\",nonce=\"" << _auth_nonce << "\"" });
    } else {
        // Of course we also support base64 authentication, but we don't recommend it
        sendRtspResponse("401 Unauthorized", { "WWW-Authenticate", StrPrinter << "Basic realm=\"" << realm << "\"" });
    }
    if (close) {
        shutdown(SockException(Err_shutdown, StrPrinter << "401 Unauthorized:" << why));
    }
}

void RtspSession::onAuthBasic(const string &realm, const string &auth_base64) {
    // Base64 authentication
    auto user_passwd = decodeBase64(auth_base64);
    auto user_pwd_vec = split(user_passwd, ":");
    if (user_pwd_vec.size() < 2) {
        // Invalid authentication information format, reply 401 Unauthorized
        onAuthFailed(realm, "can not find user and passwd when basic64 auth");
        return;
    }
    auto user = user_pwd_vec[0];
    auto pwd = user_pwd_vec[1];
    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    onAuth invoker = [pwd, realm, weak_self](bool encrypted, const string &good_pwd) {
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            // This object has been destroyed
            return;
        }
        // Switch to own thread for execution
        strong_self->async([weak_self, good_pwd, pwd, realm]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                // This object has been destroyed
                return;
            }
            // base64 ignores the encrypted parameter, the upper layer must pass in the clear text password
            if (pwd == good_pwd) {
                // The provided password matches correctly
                strong_self->onAuthSuccess();
                return;
            }
            // Password error
            strong_self->onAuthFailed(realm, StrPrinter << "password mismatch when base64 auth:" << pwd << " != " << good_pwd);
        });
    };

    // At this time, a clear text password must be provided
    if (!NOTICE_EMIT(BroadcastOnRtspAuthArgs, Broadcast::kBroadcastOnRtspAuth, _media_info, realm, user, true, invoker, *this)) {
        // This indicates that the stream requires authentication but the request password event is not monitored, which is generally done by a careless program, so a warning is issued.
        WarnP(this) << "Please listen for the kBroadcastOnRtspAuth event!";
        // But we still ignore authentication to complete playback
        // The password we entered is clear text
        invoker(false, pwd);
    }
}

void RtspSession::onAuthDigest(const string &realm,const string &auth_md5){
    DebugP(this) << auth_md5;
    auto mapTmp = Parser::parseArgs(auth_md5, ",", "=");
    decltype(mapTmp) map;
    for(auto &pr : mapTmp){
        map[trim(string(pr.first)," \"")] = trim(pr.second," \"");
    }
    //check realm
    if(realm != map["realm"]){
        onAuthFailed(realm,StrPrinter << "realm not mached:" << realm << " != " << map["realm"]);
        return ;
    }
    //check nonce
    auto nonce = map["nonce"];
    if(_auth_nonce != nonce){
        onAuthFailed(realm,StrPrinter << "nonce not mached:" << nonce << " != " << _auth_nonce);
        return ;
    }
    //check username and uri
    auto username = map["username"];
    auto uri = map["uri"];
    auto response = map["response"];
    if(username.empty() || uri.empty() || response.empty()){
        onAuthFailed(realm,StrPrinter << "username/uri/response empty:" << username << "," << uri << "," << response);
        return ;
    }

    auto realInvoker = [this,realm,nonce,uri,username,response](bool ignoreAuth,bool encrypted,const string &good_pwd){
        if(ignoreAuth){
            // Ignore authentication
            TraceP(this) << "auth ignored";
            onAuthSuccess();
            return;
        }
        /*
        The response calculation method is as follows:
        RTSP client should use username + password and calculate the response as follows:
        (1) When the password is MD5 encoded, then
            response = md5( password:nonce:md5(public_method:url)  );
        (2) When the password is an ANSI string, then
            response= md5( md5(username:realm:password):nonce:md5(public_method:url) );
         */
        auto encrypted_pwd = good_pwd;
        if(!encrypted){
            // The provided password is in clear text
            encrypted_pwd = MD5(username+ ":" + realm + ":" + good_pwd).hexdigest();
        }

        auto good_response = MD5( encrypted_pwd + ":" + nonce + ":" + MD5(string("DESCRIBE") + ":" + uri).hexdigest()).hexdigest();
        if(strcasecmp(good_response.data(),response.data()) == 0){
            // Authentication successful! md5 is case-insensitive
            onAuthSuccess();
        }else{
            // Authentication failed!
            onAuthFailed(realm, StrPrinter << "password mismatch when md5 auth:" << good_response << " != " << response );
        }
    };

    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    onAuth invoker = [realInvoker,weak_self](bool encrypted,const string &good_pwd){
        auto strong_self = weak_self.lock();
        if(!strong_self){
            return;
        }
        // Switch to its own thread to ensure that the this pointer is valid when realInvoker is executed
        strong_self->async([realInvoker,weak_self,encrypted,good_pwd](){
            auto strong_self = weak_self.lock();
            if(!strong_self){
                return;
            }
            realInvoker(false,encrypted,good_pwd);
        });
    };

    // At this time, a clear text or md5 encrypted password can be provided
    if(!NOTICE_EMIT(BroadcastOnRtspAuthArgs, Broadcast::kBroadcastOnRtspAuth, _media_info, realm, username, false, invoker, *this)){
        // This indicates that the stream requires authentication but the request password event is not monitored, which is generally done by a careless program, so a warning is issued.
        WarnP(this) << "Please listen for the kBroadcastOnRtspAuth event!";
        // But we still ignore authentication to complete playback
        realInvoker(true,true,"");
    }
}

void RtspSession::onAuthUser(const string &realm,const string &authorization){
    if(authorization.empty()){
        onAuthFailed(realm,"", false);
        return;
    }
    // The request contains authentication information
    auto authType = findSubString(authorization.data(), NULL, " ");
    auto authStr = findSubString(authorization.data(), " ", NULL);
    if(authType.empty() || authStr.empty()){
        // Invalid authentication information format, reply 401 Unauthorized
        onAuthFailed(realm,"can not find auth type or auth string");
        return;
    }
    if(authType == "Basic"){
        // base64 authentication, clear text password required
        onAuthBasic(realm,authStr);
    }else if(authType == "Digest"){
        // md5 authentication
        onAuthDigest(realm,authStr);
    }else{
        // Other authentication methods? Not supported!
        onAuthFailed(realm,StrPrinter << "unsupported auth type:" << authType);
    }
}

void RtspSession::send_StreamNotFound() {
    sendRtspResponse("404 Stream Not Found",{"Connection","Close"});
}

void RtspSession::send_UnsupportedTransport() {
    sendRtspResponse("461 Unsupported Transport",{"Connection","Close"});
}

void RtspSession::send_SessionNotFound() {
    sendRtspResponse("454 Session Not Found",{"Connection","Close"});
}

void RtspSession::handleReq_Setup(const Parser &parser) {
    // Process the setup command, this function may be entered multiple times
    int trackIdx = getTrackIndexByControlUrl(parser.fullUrl());
    SdpTrack::Ptr &trackRef = _sdp_track[trackIdx];
    if (trackRef->_inited) {
        // This Track has been initialized
        throw SockException(Err_shutdown, "can not setup one track twice");
    }

    static auto getRtpTypeStr = [](const int type) {
        switch (type)
        {
        case Rtsp::RTP_TCP:
            return "TCP";
        case Rtsp::RTP_UDP:
            return "UDP";
        case Rtsp::RTP_MULTICAST:
            return "MULTICAST";
        default:
            return "Invalid";
        }
    };

    if (_rtp_type == Rtsp::RTP_Invalid) {
        auto &strTransport = parser["Transport"];
        auto rtpType = Rtsp::RTP_Invalid;
        if (strTransport.find("TCP") != string::npos) {
            rtpType = Rtsp::RTP_TCP;
        } else if (strTransport.find("multicast") != string::npos) {
            rtpType = Rtsp::RTP_MULTICAST;
        } else {
            rtpType = Rtsp::RTP_UDP;
        }
        // Check RTP transport type restrictions
        GET_CONFIG(int, transport, Rtsp::kRtpTransportType);
        if (transport != Rtsp::RTP_Invalid && transport != rtpType) {
            WarnL << "rtsp client setup transport " << getRtpTypeStr(rtpType) << " but config force transport " << getRtpTypeStr(transport);
            // The configuration limits the RTSP transport mode, but the client's handshake mode is inconsistent, return 461
            sendRtspResponse("461 Unsupported transport");
            return;
        }
        _rtp_type = rtpType;
    }

    trackRef->_inited = true; // Initialize now

    // Allow receiving rtp, rtcp packets
    RtspSplitter::enableRecvRtp(_rtp_type == Rtsp::RTP_TCP);

    switch (_rtp_type) {
    case Rtsp::RTP_TCP: {
        if (_push_src) {
            // When pushing via RTSP, interleaved is determined by the pusher
            auto key_values = Parser::parseArgs(parser["Transport"], ";", "=");
            int interleaved_rtp = -1, interleaved_rtcp = -1;
            if (2 == sscanf(key_values["interleaved"].data(), "%d-%d", &interleaved_rtp, &interleaved_rtcp)) {
                trackRef->_interleaved = interleaved_rtp;
            } else {
                throw SockException(Err_shutdown, "can not find interleaved when setup of rtp over tcp");
            }
        } else {
            // When playing via RTSP, interleaved must be determined by the server due to shared data distribution
            trackRef->_interleaved = 2 * trackRef->_type;
        }
        sendRtspResponse("200 OK",
                         {"Transport", StrPrinter << "RTP/AVP/TCP;unicast;"
                                                  << "interleaved=" << (int) trackRef->_interleaved << "-"
                                                  << (int) trackRef->_interleaved + 1 << ";"
                                                  << "ssrc=" << printSSRC(trackRef->_ssrc),
                          "x-Transport-Options", "late-tolerance=1.400000",
                          "x-Dynamic-Rate", "1"
                         });
    }
        break;

    case Rtsp::RTP_UDP: {
        std::pair<Socket::Ptr, Socket::Ptr> pr = std::make_pair(createSocket(),createSocket());
        try {
            makeSockPair(pr, get_local_ip());
        } catch (std::exception &ex) {
            // Failed to allocate port
            send_NotAcceptable();
            throw SockException(Err_shutdown, ex.what());
        }

        _rtp_socks[trackIdx] = pr.first;
        _rtcp_socks[trackIdx] = pr.second;

        // Set client's internal network port information
        string strClientPort = findSubString(parser["Transport"].data(), "client_port=", NULL);
        uint16_t ui16RtpPort = atoi(findSubString(strClientPort.data(), NULL, "-").data());
        uint16_t ui16RtcpPort = atoi(findSubString(strClientPort.data(), "-", NULL).data());

        auto peerAddr = SockUtil::make_sockaddr(get_peer_ip().data(), ui16RtpPort);
        // Set rtp sending target address
        pr.first->bindPeerAddr((struct sockaddr *) (&peerAddr), 0, true);

        // Set rtcp sending target address
        peerAddr = SockUtil::make_sockaddr(get_peer_ip().data(), ui16RtcpPort);
        pr.second->bindPeerAddr((struct sockaddr *) (&peerAddr), 0, true);

        // Try to get the client's NAT mapping address
        startListenPeerUdpData(trackIdx);
        //InfoP(this) << "Allocated port:" << srv_port;

        sendRtspResponse("200 OK",
                         {"Transport", StrPrinter << "RTP/AVP/UDP;unicast;"
                                                  << "client_port=" << strClientPort << ";"
                                                  << "server_port=" << pr.first->get_local_port() << "-"
                                                  << pr.second->get_local_port() << ";"
                                                  << "ssrc=" << printSSRC(trackRef->_ssrc)
                         });
    }
        break;
    case Rtsp::RTP_MULTICAST: {
        if(!_multicaster){
            _multicaster = RtpMultiCaster::get(*this, get_local_ip(), _media_info, _multicast_ip, _multicast_video_port, _multicast_audio_port);
            if (!_multicaster) {
                send_NotAcceptable();
                throw SockException(Err_shutdown, "can not get a available udp multicast socket");
            }
            weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
            _multicaster->setDetachCB(this, [weak_self]() {
                auto strong_self = weak_self.lock();
                if(!strong_self) {
                    return;
                }
                strong_self->safeShutdown(SockException(Err_shutdown,"ring buffer detached"));
            });
        }
        int iSrvPort = _multicaster->getMultiCasterPort(trackRef->_type);
        // We use trackIdx to distinguish between rtp and rtcp packets
        // Since the multicast UDP port is shared, and the RTCP port is the multicast UDP port + 1, the RTCP port needs to be changed to a shared port
        auto pSockRtcp = UDPServer::Instance().getSock(*this, get_local_ip().data(), 2 * trackIdx + 1, iSrvPort + 1);
        if (!pSockRtcp) {
            // Failed to allocate port
            send_NotAcceptable();
            throw SockException(Err_shutdown, "open shared rtcp socket failed");
        }
        startListenPeerUdpData(trackIdx);
        GET_CONFIG(uint32_t,udpTTL,MultiCast::kUdpTTL);

        sendRtspResponse("200 OK",
                         {"Transport", StrPrinter << "RTP/AVP;multicast;"
                                                  << "destination=" << _multicaster->getMultiCasterIP() << ";"
                                                  << "source=" << get_local_ip() << ";"
                                                  << "port=" << iSrvPort << "-" << pSockRtcp->get_local_port() << ";"
                                                  << "ttl=" << udpTTL << ";"
                                                  << "ssrc=" << printSSRC(trackRef->_ssrc)
                         });
    }
        break;
    default:
        break;
    }
}

void RtspSession::handleReq_Play(const Parser &parser) {
    if (_sdp_track.empty() || parser["Session"] != _sessionid) {
        send_SessionNotFound();
        throw SockException(Err_shutdown, _sdp_track.empty() ? "can not find any available track when play" : "session not found when play");
    }
    auto play_src = _play_src.lock();
    if(!play_src){
        send_StreamNotFound();
        shutdown(SockException(Err_shutdown,"rtsp stream released"));
        return;
    }

    bool use_gop = true;
    auto &strScale = parser["Scale"];
    auto &strRange = parser["Range"];
    StrCaseMap res_header;
    if (!strScale.empty()) {
        // This is to set the playback speed
        res_header.emplace("Scale", strScale);
        auto speed = atof(strScale.data());
        play_src->speed(speed);
        InfoP(this) << "rtsp set play speed:" << speed;
    }

    if (!strRange.empty()) {
        // This is a seek operation
        res_header.emplace("Range", strRange);
        auto strStart = findSubString(strRange.data(), "npt=", "-");
        if (strStart == "now") {
            strStart = "0";
        }
        auto iStartTime = 1000 * (float) atof(strStart.data());
        use_gop = !play_src->seekTo((uint32_t) iStartTime);
        InfoP(this) << "rtsp seekTo(ms):" << iStartTime;
    }

    vector<TrackType> inited_tracks;
    _StrPrinter rtp_info;
    for (auto &track : _sdp_track) {
        if (track->_inited == false) {
            // To support players playing a single track, do not verify tracks that have not sent setup
            continue;
        }
        inited_tracks.emplace_back(track->_type);
        track->_ssrc = play_src->getSsrc(track->_type);
        track->_seq = play_src->getSeqence(track->_type);
        track->_time_stamp = play_src->getTimeStamp(track->_type);

        rtp_info << "url=" << track->getControlUrl(_content_base) << ";"
                 << "seq=" << track->_seq << ";"
                 << "rtptime=" << (int64_t)(track->_time_stamp) * (int64_t)(track->_samplerate/ 1000) << ",";
    }

    rtp_info.pop_back();

    res_header.emplace("RTP-Info", rtp_info);
    // Do not overwrite if Range already exists
    res_header.emplace("Range", StrPrinter << "npt=" << setiosflags(ios::fixed) << setprecision(2) << play_src->getTimeStamp(TrackInvalid) / 1000.0);
    sendRtspResponse("200 OK", res_header);

    // Set playback track
    if (inited_tracks.size() == 1) {
        _target_play_track = inited_tracks[0];
        InfoP(this) << "Specified playback track:" << _target_play_track;
    }

    // Resume playback after replying to RTSP signaling
    play_src->pause(false);

    setSocketFlags();

    if (!_play_reader && _rtp_type != Rtsp::RTP_MULTICAST) {
        weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
        _play_reader = play_src->getRing()->attach(getPoller(), use_gop);
        _play_reader->setGetInfoCB([weak_self]() {
            Any ret;
            ret.set(static_pointer_cast<Session>(weak_self.lock()));
            return ret;
        });
        _play_reader->setDetachCB([weak_self]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                return;
            }
            strong_self->shutdown(SockException(Err_shutdown, "rtsp ring buffer detached"));
        });
        _play_reader->setReadCB([weak_self](const RtspMediaSource::RingDataType &pack) {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                return;
            }
            strong_self->sendRtpPacket(pack);
        });
    }
}

void RtspSession::handleReq_Pause(const Parser &parser) {
    if (parser["Session"] != _sessionid) {
        send_SessionNotFound();
        throw SockException(Err_shutdown, "session not found when pause");
    }

    sendRtspResponse("200 OK");
    auto play_src = _play_src.lock();
    if (play_src) {
        play_src->pause(true);
    }
}

void RtspSession::handleReq_Teardown(const Parser &parser) {
    _push_src = nullptr;
    // At this time, the reply may trigger a broken pipe event, which will directly trigger the onError callback; so you need to set _push_src to null to prevent triggering the function of continuing to push after the stream is broken.
    sendRtspResponse("200 OK");
    throw SockException(Err_shutdown,"recv teardown request");
}

void RtspSession::handleReq_Get(const Parser &parser) {
    _http_x_sessioncookie = parser["x-sessioncookie"];
    sendRtspResponse("200 OK",
                     {"Cache-Control","no-store",
                      "Pragma","no-store",
                      "Content-Type","application/x-rtsp-tunnelled",
                     },"","HTTP/1.0");

    // Register http getter so that http poster can bind
    lock_guard<recursive_mutex> lock(g_mtxGetter);
    g_mapGetter[_http_x_sessioncookie] = static_pointer_cast<RtspSession>(shared_from_this());
}

void RtspSession::handleReq_Post(const Parser &parser) {
    lock_guard<recursive_mutex> lock(g_mtxGetter);
    string sessioncookie = parser["x-sessioncookie"];
    // Poster finds Getter
    auto it = g_mapGetter.find(sessioncookie);
    if (it == g_mapGetter.end()) {
        throw SockException(Err_shutdown,"can not find http getter by x-sessioncookie");
    }

    // Poster finds Getter's SOCK
    auto httpGetterWeak = it->second;
    // Remove weak reference record of http getter
    g_mapGetter.erase(sessioncookie);

    // After receiving the request, the http poster forwards it to the http getter for processing
    _on_recv = [this,httpGetterWeak](const Buffer::Ptr &buf){
        auto httpGetterStrong = httpGetterWeak.lock();
        if(!httpGetterStrong){
            shutdown(SockException(Err_shutdown,"http getter released"));
            return;
        }

        // Switch to the http getter's thread
        httpGetterStrong->async([buf,httpGetterWeak](){
            auto httpGetterStrong = httpGetterWeak.lock();
            if(!httpGetterStrong){
                return;
            }
            httpGetterStrong->onRecv(std::make_shared<BufferString>(decodeBase64(string(buf->data(), buf->size()))));
        });
    };

    if(!parser.content().empty()){
        // Packet sticking behind http poster
        _on_recv(std::make_shared<BufferString>(parser.content()));
    }

    sendRtspResponse("200 OK",
                     {"Cache-Control","no-store",
                      "Pragma","no-store",
                      "Content-Type","application/x-rtsp-tunnelled",
                     },"","HTTP/1.0");
}

void RtspSession::handleReq_SET_PARAMETER(const Parser &parser) {
    //TraceP(this) <<endl;
    sendRtspResponse("200 OK");
}

void RtspSession::send_NotAcceptable() {
    sendRtspResponse("406 Not Acceptable",{"Connection","Close"});
}

void RtspSession::onRtpSorted(RtpPacket::Ptr rtp, int track_idx) {
    if (_push_src) {
        _push_src->onWrite(std::move(rtp), false);
    } else {
        WarnL << "Not a rtsp push!";
    }
}

void RtspSession::onRcvPeerUdpData(int interleaved, const Buffer::Ptr &buf, const struct sockaddr_storage &addr) {
    // This is an RTCP heartbeat packet, indicating that the player is still alive
    _alive_ticker.resetTime();

    if (interleaved % 2 == 0) {
        if (_push_src) {
            // This is an RTP packet from an RTSP push stream
            auto &ref = _sdp_track[interleaved / 2];
            handleOneRtp(interleaved / 2, ref->_type, ref->_samplerate, (uint8_t *) buf->data(), buf->size());
        } else if (!_udp_connected_flags.count(interleaved)) {
            // This is an RTP hole-punching packet from an RTSP player
            _udp_connected_flags.emplace(interleaved);
            if (_rtp_socks[interleaved / 2]) {
                _rtp_socks[interleaved / 2]->bindPeerAddr((struct sockaddr *)&addr);
            }
        }
    } else {
        // RTCP packet
        if (!_udp_connected_flags.count(interleaved)) {
            _udp_connected_flags.emplace(interleaved);
            if (_rtcp_socks[(interleaved - 1) / 2]) {
                _rtcp_socks[(interleaved - 1) / 2]->bindPeerAddr((struct sockaddr *)&addr);
            }
        }
        onRtcpPacket((interleaved - 1) / 2, _sdp_track[(interleaved - 1) / 2], buf->data(), buf->size());
    }
}

void RtspSession::startListenPeerUdpData(int track_idx) {
    weak_ptr<RtspSession> weak_self = static_pointer_cast<RtspSession>(shared_from_this());
    auto peer_ip = get_peer_ip();
    auto onUdpData = [weak_self,peer_ip](const Buffer::Ptr &buf, struct sockaddr *peer_addr, int interleaved){
        auto strong_self = weak_self.lock();
        if (!strong_self) {
            return false;
        }

        if (SockUtil::inet_ntoa(peer_addr) != peer_ip) {
            WarnP(strong_self.get()) << ((interleaved % 2 == 0) ? "Received rtp data from other address:" : "Received rtcp data from other address:")
                                    << SockUtil::inet_ntoa(peer_addr);
            return true;
        }

        struct sockaddr_storage addr = *((struct sockaddr_storage *)peer_addr);
        strong_self->async([weak_self, buf, addr, interleaved]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                return;
            }
            try {
                strong_self->onRcvPeerUdpData(interleaved, buf, addr);
            } catch (SockException &ex) {
                strong_self->shutdown(ex);
            } catch (std::exception &ex) {
                strong_self->shutdown(SockException(Err_other, ex.what()));
            }
        });
        return true;
    };

    switch (_rtp_type){
        case Rtsp::RTP_MULTICAST:{
            // Shared RTCP port for multicast
            UDPServer::Instance().listenPeer(get_peer_ip().data(), this,
                    [onUdpData]( int interleaved, const Buffer::Ptr &buf, struct sockaddr *peer_addr) {
                return onUdpData(buf, peer_addr, interleaved);
            });
        }
            break;
        case Rtsp::RTP_UDP:{
            auto setEvent = [&](Socket::Ptr &sock,int interleaved){
                if(!sock){
                    WarnP(this) << "udp port is empty:" << interleaved;
                    return;
                }
                sock->setOnRead([onUdpData,interleaved](const Buffer::Ptr &pBuf, struct sockaddr *pPeerAddr , int addr_len){
                    onUdpData(pBuf, pPeerAddr, interleaved);
                });
            };
            setEvent(_rtp_socks[track_idx], 2 * track_idx );
            setEvent(_rtcp_socks[track_idx], 2 * track_idx + 1 );
        }
            break;

        default:
            break;
    }

}

static string dateStr(){
    char buf[64];
    time_t tt = time(NULL);
    strftime(buf, sizeof buf, "%a, %b %d %Y %H:%M:%S GMT", gmtime(&tt));
    return buf;
}

bool RtspSession::sendRtspResponse(const string &res_code, const StrCaseMap &header_const, const string &sdp, const char *protocol){
    auto header = header_const;
    header.emplace("CSeq",StrPrinter << _cseq);
    if(!_sessionid.empty()){
        header.emplace("Session", _sessionid);
    }

    header.emplace("Server",kServerName);
    header.emplace("Date",dateStr());

    if(!sdp.empty()){
        header.emplace("Content-Length",StrPrinter << sdp.size());
        header.emplace("Content-Type","application/sdp");
    }

    _StrPrinter printer;
    printer << protocol << " " << res_code << "\r\n";
    for (auto &pr : header){
        printer << pr.first << ": " << pr.second << "\r\n";
    }

    printer << "\r\n";

    if(!sdp.empty()){
        printer << sdp;
    }
//	DebugP(this) << printer;
    return send(std::make_shared<BufferString>(std::move(printer))) > 0 ;
}

ssize_t RtspSession::send(Buffer::Ptr pkt){
//	if(!_enableSendRtp){
//		DebugP(this) << pkt->data();
//	}
    _bytes_usage += pkt->size();
    return Session::send(std::move(pkt));
}

bool RtspSession::sendRtspResponse(const string &res_code, const std::initializer_list<string> &header, const string &sdp, const char *protocol) {
    string key;
    StrCaseMap header_map;
    int i = 0;
    for(auto &val : header){
        if(++i % 2 == 0){
            header_map.emplace(key,val);
        }else{
            key = val;
        }
    }
    return sendRtspResponse(res_code,header_map,sdp,protocol);
}

int RtspSession::getTrackIndexByPT(int pt) const {
    for (size_t i = 0; i < _sdp_track.size(); ++i) {
        if (pt == _sdp_track[i]->_pt) {
            return i;
        }
    }
    if (_sdp_track.size() == 1) {
        return 0;
    }
    throw SockException(Err_shutdown, StrPrinter << "no such track with pt:" << pt);
}

int RtspSession::getTrackIndexByTrackType(TrackType type) {
    for (size_t i = 0; i < _sdp_track.size(); ++i) {
        if (type == _sdp_track[i]->_type) {
            return i;
        }
    }
    if (_sdp_track.size() == 1) {
        return 0;
    }
    throw SockException(Err_shutdown, StrPrinter << "no such track with type:" << getTrackString(type));
}

int RtspSession::getTrackIndexByControlUrl(const string &control_url) {
    for (size_t i = 0; i < _sdp_track.size(); ++i) {
        if (control_url.find(_sdp_track[i]->getControlUrl(_content_base)) == 0) {
            return i;
        }
    }
    if (_sdp_track.size() == 1) {
        return 0;
    }
    throw SockException(Err_shutdown, StrPrinter << "no such track with control url:" << control_url);
}

int RtspSession::getTrackIndexByInterleaved(int interleaved) {
    for (size_t i = 0; i < _sdp_track.size(); ++i) {
        if (_sdp_track[i]->_interleaved == interleaved) {
            return i;
        }
    }
    if (_sdp_track.size() == 1) {
        return 0;
    }
    throw SockException(Err_shutdown, StrPrinter << "no such track with interleaved:" << interleaved);
}

bool RtspSession::close(MediaSource &sender) {
    // This callback is triggered in other threads
    string err = StrPrinter << "close media: " << sender.getUrl();
    safeShutdown(SockException(Err_shutdown,err));
    return true;
}

int RtspSession::totalReaderCount(MediaSource &sender) {
    return _push_src ? _push_src->totalReaderCount() : sender.readerCount();
}

MediaOriginType RtspSession::getOriginType(MediaSource &sender) const{
    return MediaOriginType::rtsp_push;
}

string RtspSession::getOriginUrl(MediaSource &sender) const {
    return _media_info.full_url;
}

std::shared_ptr<SockInfo> RtspSession::getOriginSock(MediaSource &sender) const {
    return const_cast<RtspSession *>(this)->shared_from_this();
}

toolkit::EventPoller::Ptr RtspSession::getOwnerPoller(MediaSource &sender) {
    return getPoller();
}

void RtspSession::onBeforeRtpSorted(const RtpPacket::Ptr &rtp, int track_index){
    updateRtcpContext(rtp);
}

void RtspSession::updateRtcpContext(const RtpPacket::Ptr &rtp){
    int track_index = getTrackIndexByTrackType(rtp->type);
    auto &rtcp_ctx = _rtcp_context[track_index];
    rtcp_ctx->onRtp(rtp->getSeq(), rtp->getStamp(), rtp->ntp_stamp, rtp->sample_rate, rtp->size() - RtpPacket::kRtpTcpHeaderSize);
    if (!rtp->ntp_stamp && !rtp->getStamp()) {
        // Ignore rtp with timestamp of 0
        return;
    }

    auto &ticker = _rtcp_send_tickers[track_index];
    //send rtcp every 5 second
    if (ticker.elapsedTime() > 5 * 1000 || (_send_sr_rtcp[track_index] && !_push_src)) {
        // Ensure that a sender report rtcp is sent once before sending rtp (for player audio and video synchronization)
        ticker.resetTime();
        _send_sr_rtcp[track_index] = false;

        static auto send_rtcp = [](RtspSession *thiz, int index, Buffer::Ptr ptr) {
            if (thiz->_rtp_type == Rtsp::RTP_TCP) {
                auto &track = thiz->_sdp_track[index];
                thiz->send(makeRtpOverTcpPrefix((uint16_t)(ptr->size()), track->_interleaved + 1));
                thiz->send(std::move(ptr));
            } else {
                thiz->_rtcp_socks[index]->send(std::move(ptr));
            }
        };

        auto ssrc = rtp->getSSRC();
        auto rtcp = _push_src ?  rtcp_ctx->createRtcpRR(ssrc + 1, ssrc) : rtcp_ctx->createRtcpSR(ssrc);
        auto rtcp_sdes = RtcpSdes::create({kServerName});
        rtcp_sdes->chunks.type = (uint8_t)SdesType::RTCP_SDES_CNAME;
        rtcp_sdes->chunks.ssrc = htonl(ssrc);
        send_rtcp(this, track_index, std::move(rtcp));
        send_rtcp(this, track_index, RtcpHeader::toBuffer(rtcp_sdes));
    }
}

void RtspSession::sendRtpPacket(const RtspMediaSource::RingDataType &pkt) {
    switch (_rtp_type) {
        case Rtsp::RTP_TCP: {
            setSendFlushFlag(false);
            pkt->for_each([&](const RtpPacket::Ptr &rtp) {
                if (_target_play_track == TrackInvalid || _target_play_track == rtp->type) {
                    updateRtcpContext(rtp);
                    send(rtp);
                }
            });
            flushAll();
            setSendFlushFlag(true);
        }
            break;
        case Rtsp::RTP_UDP: {
            // Subscript 0 for video, 1 for audio
            Socket::Ptr rtp_socks[2];
            rtp_socks[TrackVideo] = _rtp_socks[getTrackIndexByTrackType(TrackVideo)];
            rtp_socks[TrackAudio] = _rtp_socks[getTrackIndexByTrackType(TrackAudio)];
            pkt->for_each([&](const RtpPacket::Ptr &rtp) {
                if (_target_play_track == TrackInvalid || _target_play_track == rtp->type) {
                    updateRtcpContext(rtp);
                    auto &sock = rtp_socks[rtp->type];
                    if (!sock) {
                        shutdown(SockException(Err_shutdown, "udp sock not opened yet"));
                        return;
                    }
                    _bytes_usage += rtp->size() - RtpPacket::kRtpTcpHeaderSize;
                    sock->send(std::make_shared<BufferRtp>(rtp, RtpPacket::kRtpTcpHeaderSize), nullptr, 0, false);
                }
            });
            for (auto &sock : rtp_socks) {
                if (sock) {
                    sock->flushAll();
                }
            }
        }
            break;
        default:
            break;
    }
}

void RtspSession::setSocketFlags(){
    GET_CONFIG(int, mergeWriteMS, General::kMergeWriteMS);
    if(mergeWriteMS > 0) {
        // In push mode, disabling TCP_NODELAY will increase the delay on the push end, but the server performance will be improved
        SockUtil::setNoDelay(getSock()->rawFD(), false);
        // In playback mode, enabling MSG_MORE will increase the delay, but can improve sending performance
        setSendFlags(SOCKET_DEFAULE_FLAGS | FLAG_MORE);
    }
}

}
/* namespace mediakit */
