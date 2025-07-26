/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */
#include <mutex>
#include "Util/util.h"
#include "Util/NoticeCenter.h"
#include "Network/sockutil.h"
#include "Network/Session.h"
#include "MediaSource.h"
#include "Common/config.h"
#include "Common/Parser.h"
#include "Common/MultiMediaSourceMuxer.h"
#include "Record/MP4Reader.h"
#include "PacketCache.h"

using namespace std;
using namespace toolkit;

namespace toolkit {
    StatisticImp(mediakit::MediaSource);
}

namespace mediakit {

static recursive_mutex s_media_source_mtx;
using StreamMap = unordered_map<string/*strema_id*/, weak_ptr<MediaSource> >;
using AppStreamMap = unordered_map<string/*app*/, StreamMap>;
using VhostAppStreamMap = unordered_map<string/*vhost*/, AppStreamMap>;
using SchemaVhostAppStreamMap = unordered_map<string/*schema*/, VhostAppStreamMap>;
static SchemaVhostAppStreamMap s_media_source_map;

string getOriginTypeString(MediaOriginType type){
#define SWITCH_CASE(type) case MediaOriginType::type : return #type
    switch (type) {
        SWITCH_CASE(unknown);
        SWITCH_CASE(rtmp_push);
        SWITCH_CASE(rtsp_push);
        SWITCH_CASE(rtp_push);
        SWITCH_CASE(pull);
        SWITCH_CASE(ffmpeg_pull);
        SWITCH_CASE(mp4_vod);
        SWITCH_CASE(device_chn);
        SWITCH_CASE(rtc_push);
        SWITCH_CASE(srt_push);
        default : return "unknown";
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

ProtocolOption::ProtocolOption() {
    mINI ini;
    auto &config = mINI::Instance();
    static auto sz = strlen(Protocol::kFieldName);
    for (auto it = config.lower_bound(Protocol::kFieldName); it != config.end() && start_with(it->first, Protocol::kFieldName); ++it) {
        ini.emplace(it->first.substr(sz), it->second);
    }
    load(ini);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MediaSourceNull : public MediaSource {
    MediaSourceNull() : MediaSource("schema", MediaTuple{"vhost", "app", "stream", ""}) {};
    int readerCount() override { return 0; }
};

MediaSource &MediaSource::NullMediaSource() {
    static std::shared_ptr<MediaSource> s_null = std::make_shared<MediaSourceNull>();
    return *s_null;
}

MediaSource::MediaSource(const string &schema, const MediaTuple& tuple): _tuple(tuple) {
    GET_CONFIG(bool, enableVhost, General::kEnableVhost);
    if (!enableVhost || _tuple.vhost.empty()) {
        _tuple.vhost = DEFAULT_VHOST;
    }
    _schema = schema;
    _create_stamp = time(NULL);
}

MediaSource::~MediaSource() {
    try {
        unregist();
    } catch (std::exception &ex) {
        WarnL << "Exception occurred: " << ex.what();
    }
}

std::shared_ptr<void> MediaSource::getOwnership() {
    if (_owned.test_and_set()) {
        // Already owned
        return nullptr;
    }
    weak_ptr<MediaSource> weak_self = shared_from_this();
    // Ensure that the returned Ownership smart pointer is not empty, 0x01 has no practical meaning
    return std::shared_ptr<void>((void *) 0x01, [weak_self](void *ptr) {
        auto strong_self = weak_self.lock();
        if (strong_self) {
            strong_self->_owned.clear();
        }
    });
}

size_t MediaSource::getBytesSpeed(TrackType type) {
    if(type == TrackInvalid || type == TrackMax){
        return _speed[TrackVideo].getSpeed() + _speed[TrackAudio].getSpeed();
    }
    return _speed[type].getSpeed();
}

size_t MediaSource::getTotalBytes(TrackType type) {
    if (type == TrackInvalid || type == TrackMax) {
        return _speed[TrackVideo].getTotalBytes() + _speed[TrackAudio].getTotalBytes();
    }
    return _speed[type].getTotalBytes();
}

uint64_t MediaSource::getAliveSecond() const {
    // Use Ticker object to get alive time to prevent system time modification causing rollback
    return _ticker.createdTime() / 1000;
}

vector<Track::Ptr> MediaSource::getTracks(bool ready) const {
    auto listener = _listener.lock();
    if(!listener){
        return vector<Track::Ptr>();
    }
    return listener->getMediaTracks(const_cast<MediaSource &>(*this), ready);
}

void MediaSource::setListener(const std::weak_ptr<MediaSourceEvent> &listener){
    _listener = listener;
}

std::weak_ptr<MediaSourceEvent> MediaSource::getListener() const {
    return _listener;
}

int MediaSource::totalReaderCount(){
    auto listener = _listener.lock();
    if(!listener){
        return readerCount();
    }
    return listener->totalReaderCount(*this);
}

MediaOriginType MediaSource::getOriginType() const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaOriginType::unknown;
    }
    return listener->getOriginType(const_cast<MediaSource &>(*this));
}

string MediaSource::getOriginUrl() const {
    auto listener = _listener.lock();
    if (!listener) {
        return getUrl();
    }
    auto ret = listener->getOriginUrl(const_cast<MediaSource &>(*this));
    if (!ret.empty()) {
        return ret;
    }
    return getUrl();
}

std::shared_ptr<SockInfo> MediaSource::getOriginSock() const {
    auto listener = _listener.lock();
    if (!listener) {
        return nullptr;
    }
    return listener->getOriginSock(const_cast<MediaSource &>(*this));
}

bool MediaSource::seekTo(uint32_t stamp) {
    auto listener = _listener.lock();
    if(!listener){
        return false;
    }
    return listener->seekTo(*this, stamp);
}

bool MediaSource::pause(bool pause) {
    auto listener = _listener.lock();
    if (!listener) {
        return false;
    }
    return listener->pause(*this, pause);
}

bool MediaSource::speed(float speed) {
    auto listener = _listener.lock();
    if (!listener) {
        return false;
    }
    return listener->speed(*this, speed);
}

bool MediaSource::close(bool force) {
    auto listener = _listener.lock();
    if (!listener) {
        return false;
    }
    if (!force && totalReaderCount()) {
        // Someone is watching, do not force close
        return false;
    }
    return listener->close(*this);
}

float MediaSource::getLossRate(mediakit::TrackType type) {
    auto listener = _listener.lock();
    if (!listener) {
        return -1;
    }
    return listener->getLossRate(*this, type);
}

toolkit::EventPoller::Ptr MediaSource::getOwnerPoller() {
    toolkit::EventPoller::Ptr ret;
    auto listener = _listener.lock();
    if (listener) {
        return listener->getOwnerPoller(*this);
    }
    throw std::runtime_error(toolkit::demangle(typeid(*this).name()) + "::getOwnerPoller failed: " + getUrl());
}

std::shared_ptr<MultiMediaSourceMuxer> MediaSource::getMuxer() const {
    auto listener = _listener.lock();
    return listener ? listener->getMuxer(const_cast<MediaSource&>(*this)) : nullptr;
}

std::shared_ptr<RtpProcess> MediaSource::getRtpProcess() const {
    auto listener = _listener.lock();
    return listener ? listener->getRtpProcess(const_cast<MediaSource&>(*this)) : nullptr;
}

void MediaSource::onReaderChanged(int size) {
    try {
        weak_ptr<MediaSource> weak_self = shared_from_this();
        getOwnerPoller()->async([weak_self, size]() {
            auto strong_self = weak_self.lock();
            if (!strong_self) {
                return;
            }
            auto listener = strong_self->_listener.lock();
            if (listener) {
                listener->onReaderChanged(*strong_self, size);
            }
        });
    } catch (MediaSourceEvent::NotImplemented &ex) {
        // Interface not implemented, should print exception
        WarnL << ex.what();
    } catch (...) {
        // getOwnerPoller() interface should only throw exceptions externally, not internally
        // Therefore, ignore exceptions where listener is destroyed and ownership thread cannot be obtained
    }
}

bool MediaSource::setupRecord(Recorder::type type, bool start, const string &custom_path, size_t max_second){
    auto listener = _listener.lock();
    if (!listener) {
        WarnL << "MediaSource event listener not set, setupRecord failed: " << getUrl();
        return false;
    }
    return listener->setupRecord(*this, type, start, custom_path, max_second);
}

bool MediaSource::isRecording(Recorder::type type){
    auto listener = _listener.lock();
    if(!listener){
        return false;
    }
    return listener->isRecording(*this, type);
}

void MediaSource::startSendRtp(const MediaSourceEvent::SendRtpArgs &args, const std::function<void(uint16_t, const toolkit::SockException &)> cb) {
    auto listener = _listener.lock();
    if (!listener) {
        cb(0, SockException(Err_other, "Event listener not set yet"));
        return;
    }
    return listener->startSendRtp(*this, args, cb);
}

bool MediaSource::stopSendRtp(const string &ssrc) {
    auto listener = _listener.lock();
    if (!listener) {
        return false;
    }
    return listener->stopSendRtp(*this, ssrc);
}

template<typename MAP, typename LIST, typename First, typename ...KeyTypes>
static void for_each_media_l(const MAP &map, LIST &list, const First &first, const KeyTypes &...keys) {
    if (first.empty()) {
        for (auto &pr : map) {
            for_each_media_l(pr.second, list, keys...);
        }
        return;
    }
    auto it = map.find(first);
    if (it != map.end()) {
        for_each_media_l(it->second, list, keys...);
    }
}

template<typename LIST, typename Ptr>
static void emplace_back(LIST &list, const Ptr &ptr) {
    auto src = ptr.lock();
    if (src) {
        list.emplace_back(std::move(src));
    }
}

template<typename MAP, typename LIST, typename First>
static void for_each_media_l(const MAP &map, LIST &list, const First &first) {
    if (first.empty()) {
        for (auto &pr : map) {
            emplace_back(list, pr.second);
        }
        return;
    }
    auto it = map.find(first);
    if (it != map.end()) {
        emplace_back(list, it->second);
    }
}

void MediaSource::for_each_media(const function<void(const Ptr &src)> &cb,
                                 const string &schema,
                                 const string &vhost,
                                 const string &app,
                                 const string &stream) {
    deque<Ptr> src_list;
    {
        lock_guard<recursive_mutex> lock(s_media_source_mtx);
        for_each_media_l(s_media_source_map, src_list, schema, vhost, app, stream);
    }
    for (auto &src : src_list) {
        cb(src);
    }
}

static MediaSource::Ptr find_l(const string &schema, const string &vhost_in, const string &app, const string &id, bool from_mp4) {
    string vhost = vhost_in;
    GET_CONFIG(bool, enableVhost, General::kEnableVhost);
    if(vhost.empty() || !enableVhost){
        vhost = DEFAULT_VHOST;
    }

    if (app.empty() || id.empty()) {
        // If no app and stream id are specified, then it is traversal instead of searching, so should return search failure
        return nullptr;
    }

    MediaSource::Ptr ret;
    MediaSource::for_each_media([&](const MediaSource::Ptr &src) { ret = std::move(const_cast<MediaSource::Ptr &>(src)); }, schema, vhost, app, id);

    if(!ret && from_mp4 && schema != HLS_SCHEMA){
        // Media source not found, read mp4 to create one
        // Playing hls does not trigger mp4 on-demand (because HLS can also be used for recording, not purely live)
        ret = MediaSource::createFromMP4(schema, vhost, app, id);
    }
    return ret;
}

static void findAsync_l(const MediaInfo &info, const std::shared_ptr<Session> &session, bool retry,
                        const function<void(const MediaSource::Ptr &src)> &cb){
    auto src = find_l(info.schema, info.vhost, info.app, info.stream, true);
    if (src || !retry) {
        cb(src);
        return;
    }

    GET_CONFIG(int, maxWaitMS, General::kMaxStreamWaitTimeMS);
    void *listener_tag = session.get();
    auto poller = session->getPoller();
    std::shared_ptr<atomic_flag> invoked(new atomic_flag{false});
    auto cb_once = [cb, invoked](const MediaSource::Ptr &src) {
        if (invoked->test_and_set()) {
            // Callback has already been executed
            return;
        }
        cb(src);
    };

    auto on_timeout = poller->doDelayTask(maxWaitMS, [cb_once, listener_tag]() {
        // Wait for a maximum time, if stream is not registered within this time, return empty
        NoticeCenter::Instance().delListener(listener_tag, Broadcast::kBroadcastMediaChanged);
        cb_once(nullptr);
        return 0;
    });

    auto cancel_all = [on_timeout, listener_tag]() {
        // Cancel delayed task to prevent multiple callbacks
        on_timeout->cancel();
        // Cancel media registration event listener
        NoticeCenter::Instance().delListener(listener_tag, Broadcast::kBroadcastMediaChanged);
    };

    weak_ptr<Session> weak_session = session;
    auto on_register = [weak_session, info, cb_once, cancel_all, poller](BroadcastMediaChangedArgs) {
        if (!bRegist ||
            sender.getSchema() != info.schema ||
            !equalMediaTuple(sender.getMediaTuple(), info)) {
            // Not an event of interest, ignore it
            return;
        }

        poller->async([weak_session, cancel_all, info, cb_once]() {
            cancel_all();
            if (auto strong_session = weak_session.lock()) {
                // Stream requested by player is finally registered, switch to own thread and reply
                DebugL << "Received media registration event, replying to player: " << info.getUrl();
                // Find media source again, usually can be found
                findAsync_l(info, strong_session, false, cb_once);
            }
        }, false);
    };

    // Listen for media registration events
    NoticeCenter::Instance().addListener(listener_tag, Broadcast::kBroadcastMediaChanged, on_register);

    function<void()> close_player = [cb_once, cancel_all, poller]() {
        poller->async([cancel_all, cb_once]() {
            cancel_all();
            // Tell player that stream does not exist, will immediately disconnect player
            cb_once(nullptr);
        });
    };
    // Broadcast stream not found, can immediately pull stream at this time
    NOTICE_EMIT(BroadcastNotFoundStreamArgs, Broadcast::kBroadcastNotFoundStream, info, *session, close_player);
}

void MediaSource::findAsync(const MediaInfo &info, const std::shared_ptr<Session> &session, const function<void (const Ptr &)> &cb) {
    return findAsync_l(info, session, true, cb);
}

MediaSource::Ptr MediaSource::find(const string &schema, const string &vhost, const string &app, const string &id, bool from_mp4) {
    return find_l(schema, vhost, app, id, from_mp4);
}

MediaSource::Ptr MediaSource::find(const string &vhost, const string &app, const string &stream_id, bool from_mp4) {
    auto src = MediaSource::find(RTMP_SCHEMA, vhost, app, stream_id, from_mp4);
    if (src) {
        return src;
    }
    src = MediaSource::find(RTSP_SCHEMA, vhost, app, stream_id, from_mp4);
    if (src) {
        return src;
    }
    src = MediaSource::find(TS_SCHEMA, vhost, app, stream_id, from_mp4);
    if (src) {
        return src;
    }
    src = MediaSource::find(FMP4_SCHEMA, vhost, app, stream_id, from_mp4);
    if (src) {
        return src;
    }
    src = MediaSource::find(HLS_SCHEMA, vhost, app, stream_id, from_mp4);
    if (src) {
        return src;
    }
    return MediaSource::find(HLS_FMP4_SCHEMA, vhost, app, stream_id, from_mp4);
}

void MediaSource::emitEvent(bool regist){
    auto listener = _listener.lock();
    if (listener) {
        // Trigger callback
        listener->onRegist(*this, regist);
    }
    // Trigger broadcast
    NOTICE_EMIT(BroadcastMediaChangedArgs, Broadcast::kBroadcastMediaChanged, regist, *this);
    InfoL << (regist ? "Media registration: " : "Media unregistration: ") << getUrl();
}

void MediaSource::regist() {
    {
        // Reduce mutex critical section
        lock_guard<recursive_mutex> lock(s_media_source_mtx);
        auto &ref = s_media_source_map[_schema][_tuple.vhost][_tuple.app][_tuple.stream];
        auto src = ref.lock();
        if (src) {
            if (src.get() == this) {
                return;
            }
            // Add check to prevent re-registration when current stream is already registered
            throw std::invalid_argument("media source already existed:" + getUrl());
        }
        ref = shared_from_this();
    }
    emitEvent(true);
}

template<typename MAP, typename First, typename ...KeyTypes>
static bool erase_media_source(bool &hit, const MediaSource *thiz, MAP &map, const First &first, const KeyTypes &...keys) {
    auto it = map.find(first);
    if (it != map.end() && erase_media_source(hit, thiz, it->second, keys...)) {
        map.erase(it);
    }
    return map.empty();
}

template<typename MAP, typename First>
static bool erase_media_source(bool &hit, const MediaSource *thiz, MAP &map, const First &first) {
    auto it = map.find(first);
    if (it != map.end()) {
        auto src = it->second.lock();
        if (!src || src.get() == thiz) {
            // If object has been destroyed or object is itself, then remove it
            map.erase(it);
            hit = true;
        }
    }
    return map.empty();
}

// Unregister the source
bool MediaSource::unregist() {
    bool ret = false;
    {
        // Reduce mutex critical section
        lock_guard<recursive_mutex> lock(s_media_source_mtx);
        erase_media_source(ret, this, s_media_source_map, _schema, _tuple.vhost, _tuple.app, _tuple.stream);
    }

    if (ret) {
        emitEvent(false);
    }
    return ret;
}

bool equalMediaTuple(const MediaTuple& a, const MediaTuple& b) {
    return a.vhost == b.vhost && a.app == b.app && a.stream == b.stream;
}
/////////////////////////////////////MediaInfo//////////////////////////////////////

void MediaInfo::parse(const std::string &url_in){
    full_url = url_in;
    auto url = url_in;
    auto pos = url.find("?");
    if (pos != string::npos) {
        params = url.substr(pos + 1);
        url.erase(pos);
    }

    auto schema_pos = url.find("://");
    if (schema_pos != string::npos) {
        schema = url.substr(0, schema_pos);
    } else {
        schema_pos = -3;
    }
    auto split_vec = split(url.substr(schema_pos + 3), "/");
    if (split_vec.size() > 0) {
        splitUrl(split_vec[0], host, port);
        vhost = host;
         if (vhost == "localhost" || isIP(vhost.data())) {
            // If accessing localhost or ip, then it is the default virtual host
            vhost = DEFAULT_VHOST;
        }
    }
    if (split_vec.size() > 1) {
        app = split_vec[1];
    }
    if (split_vec.size() > 2) {
        string stream_id;
        for (size_t i = 2; i < split_vec.size(); ++i) {
            stream_id.append(split_vec[i] + "/");
        }
        if (stream_id.back() == '/') {
            stream_id.pop_back();
        }
        stream = stream_id;
    }

    auto kv = Parser::parseArgs(params);
    auto it = kv.find(VHOST_KEY);
    if (it != kv.end()) {
        vhost = it->second;
    }

    GET_CONFIG(bool, enableVhost, General::kEnableVhost);
    if (!enableVhost || vhost.empty()) {
        // If virtual host is disabled or empty, set virtual host to default
        vhost = DEFAULT_VHOST;
    }
}

MediaSource::Ptr MediaSource::createFromMP4(const string &schema, const string &vhost, const string &app, const string &stream, const string &file_path , bool check_app){
    GET_CONFIG(string, appName, Record::kAppName);
    if (check_app && app != appName) {
        return nullptr;
    }
#ifdef ENABLE_MP4
    try {
        MediaTuple tuple = {vhost, app, stream, ""};
        auto reader = std::make_shared<MP4Reader>(tuple, file_path);
        reader->startReadMP4();
        return MediaSource::find(schema, vhost, app, stream);
    } catch (std::exception &ex) {
        WarnL << ex.what();
        return nullptr;
    }
#else
    WarnL << "Failed to create MP4 on-demand, please enable \"ENABLE_MP4\" option during compilation";
    return nullptr;
#endif //ENABLE_MP4
}

/////////////////////////////////////MediaSourceEvent//////////////////////////////////////

void MediaSourceEvent::onReaderChanged(MediaSource &sender, int size){
    GET_CONFIG(bool, enable, General::kBroadcastPlayerCountChanged);
    if (enable) {
        NOTICE_EMIT(BroadcastPlayerCountChangedArgs, Broadcast::kBroadcastPlayerCountChanged, sender.getMediaTuple(), sender.totalReaderCount());
    }
    if (size || sender.totalReaderCount()) {
        // Someone is still watching this video, do not trigger close event
        _async_close_timer = nullptr;
        return;
    }
    // No one is watching this video source, indicating that the source can be closed
    GET_CONFIG(string, record_app, Record::kAppName);
    GET_CONFIG(int, stream_none_reader_delay, General::kStreamNoneReaderDelayMS);
    // If mp4 on-demand, force close on-demand when no one is watching
    bool is_mp4_vod = sender.getMediaTuple().app == record_app;
    weak_ptr<MediaSource> weak_sender = sender.shared_from_this();

    _async_close_timer = std::make_shared<Timer>(stream_none_reader_delay / 1000.0f, [weak_sender, is_mp4_vod]() {
        auto strong_sender = weak_sender.lock();
        if (!strong_sender) {
            // Object has been destroyed
            return false;
        }

        if (strong_sender->totalReaderCount()) {
            // Someone is still watching this video, do not trigger close event
            return false;
        }

        if (!is_mp4_vod) {
            auto muxer = strong_sender->getMuxer();
            if (muxer && muxer->getOption().auto_close) {
                // This stream is marked as auto-close when no viewers
                WarnL << "Auto cloe stream when none reader: " << strong_sender->getUrl();
                strong_sender->close(false);
            } else {
                // During live streaming, trigger no-viewer event, let developers choose whether to close
                NOTICE_EMIT(BroadcastStreamNoneReaderArgs, Broadcast::kBroadcastStreamNoneReader, *strong_sender);
            }
        } else {
            // This is mp4 on-demand, we automatically close it
            WarnL << "MP4 on-demand with no viewers, auto-closing: " << strong_sender->getUrl();
            strong_sender->close(false);
        }
        return false;
    }, nullptr);
}

string MediaSourceEvent::getOriginUrl(MediaSource &sender) const {
    return sender.getUrl();
}

MediaOriginType MediaSourceEventInterceptor::getOriginType(MediaSource &sender) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getOriginType(sender);
    }
    return listener->getOriginType(sender);
}

string MediaSourceEventInterceptor::getOriginUrl(MediaSource &sender) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getOriginUrl(sender);
    }
    auto ret = listener->getOriginUrl(sender);
    if (!ret.empty()) {
        return ret;
    }
    return MediaSourceEvent::getOriginUrl(sender);
}

std::shared_ptr<SockInfo> MediaSourceEventInterceptor::getOriginSock(MediaSource &sender) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getOriginSock(sender);
    }
    return listener->getOriginSock(sender);
}

bool MediaSourceEventInterceptor::seekTo(MediaSource &sender, uint32_t stamp) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::seekTo(sender, stamp);
    }
    return listener->seekTo(sender, stamp);
}

bool MediaSourceEventInterceptor::pause(MediaSource &sender, bool pause) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::pause(sender, pause);
    }
    return listener->pause(sender, pause);
}

bool MediaSourceEventInterceptor::speed(MediaSource &sender, float speed) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::speed(sender, speed);
    }
    return listener->speed(sender, speed);
}

bool MediaSourceEventInterceptor::close(MediaSource &sender) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::close(sender);
    }
    return listener->close(sender);
}

int MediaSourceEventInterceptor::totalReaderCount(MediaSource &sender) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::totalReaderCount(sender);
    }
    return listener->totalReaderCount(sender);
}

void MediaSourceEventInterceptor::onReaderChanged(MediaSource &sender, int size) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::onReaderChanged(sender, size);
    }
    listener->onReaderChanged(sender, size);
}

void MediaSourceEventInterceptor::onRegist(MediaSource &sender, bool regist) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::onRegist(sender, regist);
    }
    listener->onRegist(sender, regist);
}

float MediaSourceEventInterceptor::getLossRate(MediaSource &sender, TrackType type) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getLossRate(sender, type);
    }
    return listener->getLossRate(sender, type);
}

toolkit::EventPoller::Ptr MediaSourceEventInterceptor::getOwnerPoller(MediaSource &sender) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getOwnerPoller(sender);
    }
    return listener->getOwnerPoller(sender);
}

std::shared_ptr<MultiMediaSourceMuxer> MediaSourceEventInterceptor::getMuxer(MediaSource &sender) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getMuxer(sender);
    }
    return listener->getMuxer(sender);
}

std::shared_ptr<RtpProcess> MediaSourceEventInterceptor::getRtpProcess(MediaSource &sender) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getRtpProcess(sender);
    }
    return listener->getRtpProcess(sender);
}

bool MediaSourceEventInterceptor::setupRecord(MediaSource &sender, Recorder::type type, bool start, const string &custom_path, size_t max_second) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::setupRecord(sender, type, start, custom_path, max_second);
    }
    return listener->setupRecord(sender, type, start, custom_path, max_second);
}

bool MediaSourceEventInterceptor::isRecording(MediaSource &sender, Recorder::type type) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::isRecording(sender, type);
    }
    return listener->isRecording(sender, type);
}

vector<Track::Ptr> MediaSourceEventInterceptor::getMediaTracks(MediaSource &sender, bool trackReady) const {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::getMediaTracks(sender, trackReady);
    }
    return listener->getMediaTracks(sender, trackReady);
}

void MediaSourceEventInterceptor::startSendRtp(MediaSource &sender, const MediaSourceEvent::SendRtpArgs &args, const std::function<void(uint16_t, const toolkit::SockException &)> cb) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::startSendRtp(sender, args, cb);
    }
    listener->startSendRtp(sender, args, cb);
}

bool MediaSourceEventInterceptor::stopSendRtp(MediaSource &sender, const string &ssrc) {
    auto listener = _listener.lock();
    if (!listener) {
        return MediaSourceEvent::stopSendRtp(sender, ssrc);
    }
    return listener->stopSendRtp(sender, ssrc);
}

void MediaSourceEventInterceptor::setDelegate(const std::weak_ptr<MediaSourceEvent> &listener) {
    if (listener.lock().get() == this) {
        throw std::invalid_argument("can not set self as a delegate");
    }
    _listener = listener;
}

std::shared_ptr<MediaSourceEvent> MediaSourceEventInterceptor::getDelegate() const {
    return _listener.lock();
}

/////////////////////////////////////FlushPolicy//////////////////////////////////////

static bool isFlushAble_default(bool is_video, uint64_t last_stamp, uint64_t new_stamp, size_t cache_size) {
    if (new_stamp + 500 < last_stamp) {
        // Timestamp rollback is relatively large (possibly during seek), since timestamp in RTP is PTS, may have some degree of rollback
        return true;
    }

    // Timestamp changed or cache exceeds 1024, sendmsg interface generally can only send maximum 1024 packets
    return last_stamp != new_stamp || cache_size >= 1024;
}

static bool isFlushAble_merge(bool is_video, uint64_t last_stamp, uint64_t new_stamp, size_t cache_size, int merge_ms) {
    if (new_stamp + 500 < last_stamp) {
        // Timestamp rollback is relatively large (possibly during seek), since timestamp in RTP is PTS, may have some degree of rollback
        return true;
    }

    if (new_stamp > last_stamp + merge_ms) {
        // Timestamp increment exceeds merge write threshold
        return true;
    }

    // Cache count exceeds 1024, this logic avoids memory explosion caused by streams with abnormal timestamps
    // Moreover, sendmsg interface generally can only send maximum 1024 packets
    return cache_size >= 1024;
}

bool FlushPolicy::isFlushAble(bool is_video, bool is_key, uint64_t new_stamp, size_t cache_size) {
    bool flush_flag = false;
    if (is_key && is_video) {
        // Encounter key frame, flush previous data, ensure key frame is first frame of this group, ensure GOP cache is valid
        flush_flag = true;
    } else {
        GET_CONFIG(int, mergeWriteMS, General::kMergeWriteMS);
        if (mergeWriteMS <= 0) {
            // Merge writing is disabled or merge writing threshold is less than or equal to 0
            flush_flag = isFlushAble_default(is_video, _last_stamp[is_video], new_stamp, cache_size);
        } else {
            flush_flag = isFlushAble_merge(is_video, _last_stamp[is_video], new_stamp, cache_size, mergeWriteMS);
        }
    }

    if (flush_flag) {
        _last_stamp[is_video] = new_stamp;
    }
    return flush_flag;
}

} /* namespace mediakit */