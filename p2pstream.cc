/**
 * Simulation of mesh based P2P live streaming systems
 * Copyright 2016, Midhul Varma
 * All Rights Reserved
 */

// NS3 includes
#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"

// STL includes
#include <iostream>
#include <algorithm>
#include <queue>

using namespace ns3;
// Prefix STL names with `std::`

NS_LOG_COMPONENT_DEFINE("p2pstream");

// Configurable constants

// NOTE: To adjust streaming rate, play around with MSG_SEG_SIZE & SOURCE_DURATION

// WARNING: All sizes here are in bytes

// Maximum allowed Msg Types = 256
#define NUM_MSG_TYPES 3
#define MSG_HELLO_SIZE 128
#define MSG_SEG_REQ_SIZE 32
#define MSG_SEG_SIZE 1250
#define DEFAULT_SEND_SIZE 1024
#define BASE_SEG_NUMBER 10000

// TODO: Get rid of this
#define MAX_SEGS_PER_PERIOD 350


// All durations here are in seconds
#define SOURCE_DURATION 0.02
// Source window size in secs
#define SOURCE_WINDOW_DURATION 20
#define PLAYBACK_BUFFER_DURATION 20
#define OLDCACHE_DURATION 20
#define SCHEDULE_DURATION 4.0
#define MIN_PLAYBACK_START_DURATION 2

#define NUM_VIEWERS 2


// Types of messages for the MsgSocket interface
enum MsgType {
    MSG_HELLO,
    MSG_SEG_REQ,
    MSG_SEG
};

// Mapping MsgTypes to their corresponding sizes
// This NEEDS to be inilialized in the `initGlobals()` method
uint32_t MsgTypeSize[NUM_MSG_TYPES];


// Initializing some important globals
void initGlobals() {
    // Initialize MsgTypeSize array
    MsgTypeSize[static_cast<int>(MSG_HELLO)] = MSG_HELLO_SIZE;
    MsgTypeSize[static_cast<int>(MSG_SEG_REQ)] = MSG_SEG_REQ_SIZE;
    MsgTypeSize[static_cast<int>(MSG_SEG)] = MSG_SEG_SIZE;
}



///////////////////////////////////////////////////////////////////////////////////////////////////

// Byte tag which carries MsgType and MsgAttribute information
class MsgTag : public Tag
{
public:
  static TypeId GetTypeId (void);
  virtual TypeId GetInstanceTypeId (void) const;
  virtual uint32_t GetSerializedSize (void) const;
  virtual void Serialize (TagBuffer i) const;
  virtual void Deserialize (TagBuffer i);
  virtual void Print (std::ostream &os) const;

  // public members
  // NOTE: GetSerializedSize, Serialize, Deserialize NEED 
  // to be edited if members are added or modified
  MsgType msgType;
  uint32_t msgAttribute;

  // Constructors
  MsgTag() { }
  MsgTag(MsgType _msgType, uint32_t _msgAttribute) {
    msgType = _msgType;
    msgAttribute = _msgAttribute;
  }
};

// MsgTag public method implementations
TypeId 
MsgTag::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::MsgTag")
    .SetParent<Tag> ()
    .AddConstructor<MsgTag> ();
  return tid;
}

TypeId 
MsgTag::GetInstanceTypeId (void) const
{
  return GetTypeId ();
}

// This NEEDS to be edited when members of MsgTag are modified
uint32_t 
MsgTag::GetSerializedSize (void) const
{
  return sizeof(MsgType) + sizeof(uint32_t);
}

// This NEEDS to be edited when members of MsgTag are modified
void 
MsgTag::Serialize (TagBuffer i) const
{
  i.WriteU8 ((uint8_t)msgType);
  i.WriteU32(msgAttribute);
}

// This NEEDS to be edited when members of MsgTag are modified
void 
MsgTag::Deserialize (TagBuffer i)
{
  msgType = static_cast<MsgType>((int)i.ReadU8());
  msgAttribute = i.ReadU32();
}

// This NEEDS to be edited when members of MsgTag are modified
void 
MsgTag::Print (std::ostream &os) const
{
  os << "msgType=" << (uint32_t)msgType << "; Attribute: " << msgAttribute;
}


///////////////////////////////////////////////////////////////////////////////////////////////////



// Implementation of a message passing interface on top of 
// TCP Socket's irritating byte stream interface
// Inherits from ns3::SimpleRefCount (TODO: Verify the semantics of this)
class MsgSocket: public SimpleRefCount<MsgSocket> {

// Private Members
private:
    // Smart Pointer to this
    Ptr<MsgSocket> m_thisPtr;

    // Underlying TCP socket
    Ptr<Socket> m_socket;
    // Items inside the message send queue
    class MsgQueueItem {
    public:
        MsgType msgType;
        uint32_t msgAttribute;
        // No. of bytes that are yet to be Tx'ed
        uint32_t toTxBytes;
    };

    // Maximum size of each send chunk
    uint32_t m_sendSize;

    // Queue of messages to be sent
    std::queue<MsgQueueItem> m_sendQueue;

    // No. of receiving message bytes buffered
    uint32_t m_rxBuffBytes;

    // Set of registered callbacks
    Callback< void, MsgType, uint32_t, Ptr<MsgSocket> > m_regCallbacks[NUM_MSG_TYPES];

// Private methods
private:
    // Create a packet of given type, attribute and size
    // Byte packet is attached to carry type and attribute information
    Ptr<Packet> CreateMsgPacket(MsgType msgType, uint32_t msgAttribute, uint32_t pktSize) {
        MsgTag tag(msgType, msgAttribute);

        Ptr<Packet> p = Create<Packet>(pktSize);
        p->AddByteTag(tag);

        return p;
    }

    // Send till TCP send buffer is full
    void FillSendBuffer() {
        // NS_LOG_DEBUG("FillSendBuffer called on " << this);
        while(!m_sendQueue.empty()) {
            MsgQueueItem &front = m_sendQueue.front(); 
            uint32_t toSend = std::min(front.toTxBytes, m_sendSize);
            Ptr<Packet> sendData = CreateMsgPacket(front.msgType, front.msgAttribute, toSend);

            int actual = m_socket->Send(sendData);

            if(actual > 0) {
                front.toTxBytes -= actual;
            }
            // check if the msg has been fully sent
            if(front.toTxBytes == 0) {
                m_sendQueue.pop();
            }

            // If TCP send buffer is full return
            if((unsigned) actual != toSend) {
                break;
            }
        }
    }

    // Receive till the TCP receive buffer is empty
    // NOTE: Presently there is no limit on the size of received chunk
    // TODO: Check to see if this can create problems
    // WARNING: Assuming Byte Tags are inorder of byte ranges,
    // if this is not true, them I am screwed :/
    // Can't think of a reason why they would be out of order
    // TODO: Check this
    void EmptyRecvBuffer() {
        // NS_LOG_DEBUG("EmptyRecvBuffer called on " << this);
        Ptr<Packet> p;

        while((p = m_socket->Recv())) {
            if(p->GetSize() == 0) {
                break;
            }

            // Iterate over all Byte Tags in packet
            // and classify into corresponding msg type
            ByteTagIterator iter = p->GetByteTagIterator();
            while(iter.HasNext()) {
                ByteTagIterator::Item it = iter.Next();
                MsgTag tag;
                it.GetTag(tag);

                uint32_t chunkBytes = it.GetEnd() - it.GetStart();
                uint32_t msgSize = MsgTypeSize[(int)tag.msgType];
                uint32_t attribute = tag.msgAttribute;

                // check if this is the last chunk of the message
                if(m_rxBuffBytes + chunkBytes == msgSize) {
                    // reset the buffer
                    m_rxBuffBytes = 0;

                    // Acknowledge arrival of the message
                    // Call the associated callback
                    Callback< void, MsgType, uint32_t, Ptr<MsgSocket> > cb =
                        m_regCallbacks[(int)tag.msgType];

                    if(cb.IsNull()) {
                        NS_LOG_ERROR("Received a message type with " 
                            "unregistered/null callback: (MsgType)" 
                            << (int)tag.msgType);
                    }
                    else {
                        cb(tag.msgType, attribute, m_thisPtr);
                    }   
                } 
                else {
                    // Buffer the chunk
                    m_rxBuffBytes += chunkBytes;
                }

            }
        }
    }

    // Callbacks to handle socket send and recv
    void SendCallback(Ptr<Socket>, uint32_t) {
        // TODO: ensure socket is connected
        
        FillSendBuffer();
    }

    void RecvCallback(Ptr<Socket>) {
        EmptyRecvBuffer();
    }


// Public methods
public:
    
    // Constructor
    MsgSocket(Ptr<Socket> sock) {
        // Initialize members
        m_socket = sock;
        m_sendSize = DEFAULT_SEND_SIZE;
        Ptr<MsgSocket> thisPtr(this);
        m_thisPtr = thisPtr;
        m_rxBuffBytes = 0;

        // Initialize with null callbacks
        for(int i = 0; i < NUM_MSG_TYPES; i++) {
            m_regCallbacks[i] = 
                MakeNullCallback< void, MsgType, uint32_t, Ptr<MsgSocket> >();
        }

        // Setup socket handlers
        m_socket->SetSendCallback(MakeCallback(&MsgSocket::SendCallback, this));
        m_socket->SetRecvCallback(MakeCallback(&MsgSocket::RecvCallback, this));
        
    }

    // Modify the send size
    void SetSendSize(uint32_t ss) {
        m_sendSize = ss;
    }

    // Bind to TCP socket
    void BindToSocket(Ptr<Socket> sock) {
        m_socket = sock;
    }

    // MsgSocket interface methods
    
    void SendMessage(MsgType msgType, uint32_t attribute) {
        // TODO: Ensure socket is connected
        // push to the send queue
        MsgQueueItem msgItem;
        msgItem.msgType = msgType;
        msgItem.msgAttribute = attribute;
        msgItem.toTxBytes = MsgTypeSize[(int)msgType];
        m_sendQueue.push(msgItem);

        FillSendBuffer();
    }

    void RegisterHandler(MsgType msgType, 
        Callback< void, MsgType, uint32_t, Ptr<MsgSocket> > cb) {

        m_regCallbacks[(int)msgType] = cb;
    }
};

///////////////////////////////////////////////////////////////////////////////////////////////////


// Connects TCP sockets to upstream peers,
// wraps them with MsgSocket interfaces and
// exposes these MsgSocket interfaces to the application
class PeerConnector : public SimpleRefCount<PeerConnector> {
private:
    std::vector< Ptr<MsgSocket> > m_peerMsgSockets;

    // Node to which the PeerConnector is attached
    Ptr<Node> m_node;

    // Upstream success callback
    // Callback<void, Ptr<MsgSocket> mSock > m_successCallback;

    // Successful connection callback
    void ConnectSuccess(Ptr<Socket> sock) {
        // Wrap with MsgSocket
        Ptr<MsgSocket> mSock = Create<MsgSocket>(sock);

        m_peerMsgSockets.push_back(mSock);

        // NS_LOG_DEBUG("ConnectSuccess called");

        // Trigger upstream success callback
        // m_successCallback(mSock);
        
    }

    // Successful connection w/ callback
    static void ConnectSuccessCb(PeerConnector *self, 
        Callback< void, Ptr<MsgSocket> > cb, Ptr<Socket> sock) {

        NS_LOG_DEBUG("ConnectSuccessCb called");

        // Wrap with MsgSocket
        Ptr<MsgSocket> mSock = Create<MsgSocket>(sock);

        self->AddPeerSock(mSock);

        cb(mSock);
    }

    // Failed connection
    // NOTE: As of now, should not happen
    void ConnectFailed(Ptr<Socket>) {
        NS_LOG_ERROR("Socket Connection to peer failed on PeerConnector: " << this);
    }


public:
    PeerConnector(Ptr<Node> node) {
        m_node = node;
    }

    // Add a peer message socket
    void AddPeerSock(Ptr<MsgSocket> mSock) {
        m_peerMsgSockets.push_back(mSock);
    }

    int NumPeers() {
        return m_peerMsgSockets.size();
    }

    Ptr<MsgSocket> GetPeer(int idx) {
        if(!(idx >= 0 && idx < (int)m_peerMsgSockets.size())) {
            NS_LOG_ERROR("Attempt to access invalid index peer on PeerConnector");
            Ptr<MsgSocket> nullPtr(NULL);
            return nullPtr;
        }

        return m_peerMsgSockets[idx];
    }

    // Connect to an upstream peer
    void ConnectPeer(const Address &address) {

        Ptr<Socket> sock = Socket::CreateSocket(m_node, TcpSocketFactory::GetTypeId());
        sock->Bind();
        sock->SetConnectCallback(MakeCallback(&PeerConnector::ConnectSuccess, this),
                                 MakeCallback(&PeerConnector::ConnectFailed, this));
        sock->Connect(address);
    }

    // Connect to an upstream peer with callback
    void ConnectPeer(const Address &address, Callback<void, Ptr<MsgSocket> > cb) {

        NS_LOG_FUNCTION(this);

        Ptr<Socket> sock = Socket::CreateSocket(m_node, TcpSocketFactory::GetTypeId());
        sock->Bind();
        sock->SetConnectCallback(MakeBoundCallback(&PeerConnector::ConnectSuccessCb, this, cb),
                                 MakeCallback(&PeerConnector::ConnectFailed, this));
        sock->Connect(address);
    }

};

// Listens to connections from downstream peers,
// maintains corresponsing TCP sockets,
// wraps them with MsgSockets and exposes to application
// TODO: add a way to bound the number of incoming connections
class PeerListener : public SimpleRefCount<PeerListener> {
private:
    std::vector< Ptr<MsgSocket> > m_peerMsgSockets;

    // Node to which the PeerConnector is attached
    Ptr<Node> m_node;

    // local Address to bind listener
    Address m_local;

    // Listening socket
    Ptr<Socket> m_lSock;

    // Handler for new connections
    void HandleAccept(Ptr<Socket> sock, const Address &) {
        // Wrap wih MsgSocket
        Ptr<MsgSocket> mSock = Create<MsgSocket>(sock);

        m_peerMsgSockets.push_back(mSock);
    }

    // Accept all incoming connections
    // TODO: This can be used to bound incoming connections
    bool AcceptConnection(Ptr<Socket>, const Address &) {
        return true;
    }

public:
    PeerListener(Ptr<Node> node, uint16_t port) {
        m_node = node;

        // WARNING: Check is it's okay to send any IP address
        // Also check to see if implicit type conversion is done on =
        m_local = InetSocketAddress(Ipv4Address::GetAny(), port);

        // Create a listening socket
        m_lSock = Socket::CreateSocket(m_node, TcpSocketFactory::GetTypeId());
        m_lSock->Bind(m_local);
        m_lSock->Listen();

        m_lSock->SetAcceptCallback (
            MakeCallback(&PeerListener::AcceptConnection, this),
            MakeCallback(&PeerListener::HandleAccept, this));

    }

    int NumPeers() {
        return m_peerMsgSockets.size();
    }

    Ptr<MsgSocket> GetPeer(int idx) {
        if(!(idx >= 0 && idx < (int)m_peerMsgSockets.size())) {
            NS_LOG_ERROR("Attempt to access invalid index peer on PeerConnector");
            Ptr<MsgSocket> nullPtr(NULL);
            return nullPtr;
        }

        return m_peerMsgSockets[idx];
    }

};


///////////////////////////////////////////////////////////////////////////////////////////////////


// Buffer for video chunks to be used by player
// Implemented as a circular buffer
// Buffer contains two sections:
// OldCache -> Chunks which have been consumed by player
// PlaybackBuffer -> Chunks which are yet to be playedback
// WARNING: Seek MUST be called on buffer before usage
// NOTE: Get on chunk outside range => returns 0
class VideoBuffer : public SimpleRefCount<VideoBuffer> {
private:
    // Size of the buffer sections
    uint16_t m_oldCacheSize;
    uint16_t m_playbackBufferSize;
    uint16_t m_totalSize;

    // Chunk numbers
    uint32_t m_oldestChunk; // oldest chunk
    uint32_t m_playbackChunk; // chunk which is to be played next
    uint32_t m_newestChunk; // newest chunk in the buffer

    // Starting index of circular buffer
    uint32_t m_startIdx;

    // Container for circular buffer
    std::vector<uint8_t> m_chunkBuffer;
public:

    VideoBuffer(uint16_t oldCache, uint16_t playbackBuffer) {
        m_oldCacheSize = oldCache;
        m_playbackBufferSize = playbackBuffer;
        m_totalSize = m_oldCacheSize + m_playbackBufferSize;
        m_chunkBuffer.resize(m_totalSize);

        m_startIdx = 0;

        // default chunk no.s
        m_oldestChunk = 0;
        m_playbackChunk = m_oldCacheSize;
        m_newestChunk = m_totalSize - 1;

    }

    // IMPORTANT: This must be called before the buffer is used
    // Seeks the buffer so that the next chunk to be played is seekPoint
    // WARNING: To avoid uint undeflows start with sufficiently big base segment no
    // NOTE: Buffer is cleared when Seek is called
    void Seek(uint32_t seekPoint) {
        m_playbackChunk = seekPoint;
        m_oldestChunk = seekPoint - m_oldCacheSize;
        m_newestChunk = seekPoint + m_playbackBufferSize - 1;

        // clear buffer
        std::fill(m_chunkBuffer.begin(), m_chunkBuffer.end(), 0);
    }

    // Get the status of a chunk
    uint8_t Get(uint32_t chunk) {
        // Check boundary
        if(!( chunk >= m_oldestChunk && chunk <= m_newestChunk )) {
            NS_LOG_WARN("Attempt to Get chunk outside buffer range");
            return 0;
        }
        
        int offset = chunk - m_oldestChunk;
        int idx = (m_startIdx + offset)%m_totalSize;
        return m_chunkBuffer[idx];
    }

    // Put a chunk into the buffer
    void Put(uint32_t chunk, uint8_t payload) {
        // Check boundary
        if(!( chunk >= m_oldestChunk && chunk <= m_newestChunk )) {
            NS_LOG_WARN("Attempt to Put chunk outside buffer range");
            return;
        }

        int offset = chunk - m_oldestChunk;
        int idx = (m_startIdx + offset)%m_totalSize;
        m_chunkBuffer[idx] = payload;
    }

    // Slide the buffer forward in time by one segment
    void Slide() {
        m_chunkBuffer[m_startIdx] = 0;
        m_startIdx = (m_startIdx + 1)%m_totalSize;
        m_oldestChunk += 1;
        m_playbackChunk += 1;
        m_newestChunk += 1;
    }

    // Getters
    uint32_t GetOldestChunk() {
        return m_oldestChunk;
    }

    uint32_t GetPlaybackChunk() {
        return m_playbackChunk;
    }

    uint32_t GetNewestChunk() {
        return m_newestChunk;
    }
};


// Video Source application
// Serves a livestream using a sliding window

class VideoSource : public Application {
private:

    uint16_t m_port;
    Ptr<PeerListener> m_listener;

    // chunk numbers
    uint32_t m_windowStart;
    uint32_t m_windowEnd;

    // Slide duration
    Time m_duration;

    // window size
    uint16_t m_windowSize;

    // number of chunks per period
    // These many chunks will be generated per duration
    uint16_t m_chunksPerPeriod;

    // For now I'm assuming the source has infinite cache size
    void HandleRequest(MsgType, uint32_t segment, Ptr<MsgSocket> mSock) {
        if(segment <= m_windowEnd) {
            // Send the segment as response
            mSock->SendMessage(MSG_SEG, segment);
        }
        else {
            NS_LOG_WARN("Unproduced chunk was requested from VideoSource");
        }
    }

    void Slide() {
        m_windowStart += m_chunksPerPeriod;
        m_windowEnd += m_chunksPerPeriod;

        // Repeat
        Simulator::Schedule(m_duration, &VideoSource::Slide, this);
        
    }

    // Assumption: this is called after topology is connected
    virtual void StartApplication() {
        // Register segment request handlers on
        // MsgSockets of all downstream peers
        int n = m_listener->NumPeers();
        for(int i = 0; i < n; i++) {
            Ptr<MsgSocket> mSock = m_listener->GetPeer(i);
            mSock->RegisterHandler(MSG_SEG_REQ, 
                MakeCallback(&VideoSource::HandleRequest, this));
        }

        // Schedule sliding to happen periodically
        Simulator::Schedule(m_duration, &VideoSource::Slide, this);
        
    }
    virtual void StopApplication() {
        // Nothing for now
    }

public:
    VideoSource(uint16_t port, uint32_t startSeg, Time duration,
                uint16_t windowSize, uint16_t chunksPerPeriod)
    {
        m_port = port;
        m_windowStart = startSeg;
        m_windowEnd = startSeg + windowSize - 1;
        m_duration = duration;
        m_windowSize = windowSize;
        m_chunksPerPeriod = chunksPerPeriod;
    }

    // This NEEDS to be called AFTER the app is attached to a Node
    void Setup() {
        m_listener = Create<PeerListener> (GetNode(), m_port);
    }

    // For clients to get window status
    uint32_t GetWindowStart() {
        return m_windowStart;
    }

    uint32_t GetWindowEnd() {
        return m_windowEnd;
    }

    bool HasSeg(uint32_t seg) {
        return (seg <= m_windowEnd);
    }

    int NumPeers() {
        return m_listener->NumPeers();
    }

    // Helper function to help setup app on Node
    static Ptr<VideoSource> CreateVideoSource(uint16_t port, uint32_t startSeg, Time duration,
                            uint16_t windowSize, uint16_t chunksPerPeriod, Ptr<Node> node) 
    {
        Ptr<VideoSource> app = CreateObject<VideoSource>(port, startSeg, duration,
                                windowSize, chunksPerPeriod);
        node->AddApplication(app);
        app->Setup();
        return app;
    }
};


// Basic Video Player implementation
// Inorder chunk scheduling
// TODO: Implement B/W estimation
// chunk status numbers:
// 0 -> not yet requested
// 1 -> requested but not yet received
// 2 -> received completely
class VideoPlayer : public Application {
private:
    Ptr<VideoSource> m_source;

    Ptr<PeerConnector> m_connector;

    Ptr<VideoBuffer> m_buffer;

    Ptr<PeerListener> m_listener;

    // Period between scheduling events
    Time m_sDuration;

    // Segment playback duration
    Time m_segDuration;

    // Minimum no. of segments to be buffered before playback starts
    uint16_t m_minSegs;

    // playback status
    bool m_playbackStarted;

    // No of missed segments
    uint32_t m_missedSegs;

    // listening port
    uint16_t m_port;

    // To keep track of how many segments have been scheduled per peer
    // NOTE: This should be sized appropriately in StartApplication
    std::vector<int> m_segsOnPeer;

    int m_segsInPeriod;

    // // Set of all segments which have been requested but not yet received.
    // // TODO: Keep an eye on the memory overhead of std::set
    // std::set<uint32_t> m_segsRequested; 
    
    // Uniform random number generator
    Ptr<UniformRandomVariable> m_randomVar;

    // List of peer VideoPlayer objects
    // To be used to query segment availability status
    std::vector< Ptr<VideoPlayer> > m_peers;

    // Callback when connection to peer is successful
    // static is used because bound callbacks for member functions
    // are not supported by NS3 yet :(
    static void PeerConnected(Ptr<VideoPlayer> instance, 
        Ptr<VideoPlayer> peerPlayer, Ptr<MsgSocket>) {

        instance->AddPeer(peerPlayer);

        NS_LOG_DEBUG("P2P Connection created");

    }

    

    // Whether not chunk has been fully downloaded
    bool Downloaded(uint32_t seg) {
        return m_buffer->Get(seg) == 2;
    }

    // Whether or not chunk has been requested
    bool Requested(uint32_t seg) {
        return m_buffer->Get(seg) == 1;
    }

    // If chunk has not been requested or downloaded yet
    // This needs to be modified to enable refetching of
    // chunks which have not yet been received.
    bool Untouched(uint32_t seg) {
        return m_buffer->Get(seg) == 0;
    }

    // Set segment status as downloaded
    void SetDownloaded(uint32_t seg) {
        m_buffer->Put(seg, 2);
    }

    // Set segetn status as requested
    void SetRequested(uint32_t seg) {
        m_buffer->Put(seg, 1);
    }
    

    // Checks if playback can start
    bool CanPlaybackStart() {
        // Check to see if first m_minSegs segments in playback buffer are there
        bool res = true;
        uint32_t pChunk = m_buffer->GetPlaybackChunk();
        for(int i = 0; i < m_minSegs; i++) {
            res = (res && Downloaded(pChunk + i)); 
        }
        return res;
    }

    // Consume next chunk in playback buffer
    void Consume() {
        // Check if next playback chunk is available?
        uint32_t pNext = m_buffer->GetPlaybackChunk();
        if(!Downloaded(pNext)) {
            NS_LOG_DEBUG("Missing segment during playback");
            m_missedSegs += 1;
        }

        uint32_t chunk = m_buffer->GetPlaybackChunk();
        NS_LOG_DEBUG("Playing chunk " << chunk);

        m_buffer->Slide();

        // Repeat
        Simulator::Schedule(m_segDuration, &VideoPlayer::Consume, this);

    }

    // Handle segment requests form other peers
    void HandleRequest(MsgType, uint32_t segment, Ptr<MsgSocket> mSock) {
        if(Downloaded(segment)) {
            // Send the segment as response
            mSock->SendMessage(MSG_SEG, segment);
        }
        else {
            NS_LOG_WARN("Unproduced chunk was requested from VideoPlayer");
        }
    }

    void HandleResponse(MsgType, uint32_t seg, Ptr<MsgSocket>) {
        if(Downloaded(seg)) {
            NS_LOG_WARN("Duplicate segment received by VideoPlayer");
            return;
        }

        NS_LOG_DEBUG("Received segment " << seg << " :)");

        SetDownloaded(seg);
        if(m_playbackStarted) return;
        // If playback has not started yet, see if it can start now
        if(CanPlaybackStart()) {
            NS_LOG_DEBUG("Playback has started :)");
            m_playbackStarted = true;
            Consume();
        }
    }

    // THE CRUX OF THE MATTER
    // Request segments in playback buffer from peers
    // void SegSchedule() {

    //     std::fill(m_segsOnPeer.begin(), m_segsOnPeer.end(), 0);
    //     uint32_t pSeg = m_buffer->GetPlaybackChunk();
    //     uint32_t lSeg = m_buffer->GetNewestChunk();

    //     // Segment refetching is disabled for now
    //     for(uint32_t seg = pSeg; seg <= lSeg; seg++) {
    //         if(Downloaded(seg) || Requested(seg))
    //             continue;

    //         if(seg <= m_source->GetWindowEnd() && m_segsOnPeer[0] < MAX_SEGS_PER_PERIOD) {
    //             m_connector->GetPeer(0)->SendMessage(MSG_SEG_REQ, seg);
    //             m_segsOnPeer[0] += 1;
    //             SetRequested(seg);
    //         }

    //     }

    //     // Repeat
    //     Simulator::Schedule(m_sDuration, &VideoPlayer::SegSchedule, this);
    // }

    // Random chunk scheduling
    void SegSchedule() {

        std::fill(m_segsOnPeer.begin(), m_segsOnPeer.end(), 0);
        m_segsInPeriod = 0;

        uint32_t pSeg = m_buffer->GetPlaybackChunk();
        uint32_t lSeg = m_buffer->GetNewestChunk();

        // Segment refetching is disabled for now
        for(uint32_t seg = pSeg; seg <= lSeg; seg++) {
            if(Downloaded(seg) || Requested(seg))
                continue;

            std::vector<int> holders;
            for(int peer = 0; peer < (int)m_peers.size(); peer++) {
                if(m_peers[peer]->HasSeg(seg))
                    holders.push_back(peer);
            }


            // Select a random holder
            int idx = m_randomVar->GetInteger(0, holders.size() - 1);

            if(holders.size() > 0 && m_segsInPeriod < MAX_SEGS_PER_PERIOD) {
                m_connector->GetPeer(holders[idx])->SendMessage(MSG_SEG_REQ, seg);
                // m_segsOnPeer[0] += 1;
                m_segsInPeriod += 1;
                SetRequested(seg);
            }

        }

        // Repeat
        Simulator::Schedule(m_sDuration, &VideoPlayer::SegSchedule, this);
    }

    // Assumption: Topology has already been connected
    virtual void StartApplication() {
        NS_LOG_DEBUG("Start Application called");
        int n = m_connector->NumPeers();
        m_segsOnPeer.resize(n);
        std::fill(m_segsOnPeer.begin(), m_segsOnPeer.end(), 0);

        // Register response handlers
        for(int i = 0; i < n; i++) {
            Ptr<MsgSocket> mSock = m_connector->GetPeer(i);
            mSock->RegisterHandler(MSG_SEG, 
                MakeCallback(&VideoPlayer::HandleResponse, this));
        }

        // Register segment request handlers on
        // MsgSockets of all peers
        n = m_listener->NumPeers();
        for(int i = 0; i < n; i++) {
            Ptr<MsgSocket> mSock = m_listener->GetPeer(i);
            mSock->RegisterHandler(MSG_SEG_REQ, 
                MakeCallback(&VideoPlayer::HandleRequest, this));
        }

        // Seek buffer to right point
        m_buffer->Seek(m_source->GetWindowStart());

        // Start the periodic scheduling process
        SegSchedule();


    }

    virtual void StopApplication() {
        // Nothing as of now
    }

public:
    VideoPlayer(Ptr<VideoSource> source, uint16_t oldCacheSize, 
        uint16_t playbackBufferSize, Time sDuration, Time segDuration,
        uint16_t minSegsForPlayback) 
    {
        m_buffer = Create<VideoBuffer>(oldCacheSize, playbackBufferSize);
        m_source = source;
        m_sDuration = sDuration;
        m_segDuration = segDuration;
        m_playbackStarted = false;
        m_minSegs = minSegsForPlayback;
        m_missedSegs = 0;
        // TODO: Make this customizable
        m_port = 3434;

        m_randomVar = CreateObject<UniformRandomVariable>();
    }

    // Add peer player to list
    void AddPeer(Ptr<VideoPlayer> peerPlayer) {
        m_peers.push_back(peerPlayer);
    }

    // Connect to streaming server
    void ConnectPeer(const Address &address) {
    
        // pass it onto PeerConnector
        m_connector->ConnectPeer(address);
    }

    // Connect to an upstream peer
    void ConnectPeer(const Address &address, Ptr<VideoPlayer> peerPlayer) {

        NS_LOG_FUNCTION(this);
        
        Ptr<VideoPlayer> thisPtr(this);

        // pass it onto PeerConnector
        m_connector->ConnectPeer(address,
            MakeBoundCallback(&VideoPlayer::PeerConnected, thisPtr, peerPlayer));
    }

    int NumPeers() {
        return m_connector->NumPeers();
    }

    // Whether a given segment is available or not
    bool HasSeg(uint32_t seg) {
        return Downloaded(seg);
    }

    // This MUST be called AFTER app is attached to a Node
    void Setup() {
        m_connector = Create<PeerConnector>(GetNode());
        m_listener = Create<PeerListener> (GetNode(), m_port);
    }

};

// Helper class to setup VideoPlayer
class VideoPlayerHelper {
private:
    Ptr<VideoSource> m_source;
    uint16_t m_oldCacheSize;
    uint16_t m_playbackBufferSize;
    Time m_sDuration;
    Time m_segDuration;
    uint16_t m_minSegs;

public:

    VideoPlayerHelper(Ptr<VideoSource> source, uint16_t oldCacheSize, 
        uint16_t playbackBufferSize, Time sDuration, Time segDuration,
        uint16_t minSegsForPlayback) 
    {
        m_source = source;
        m_sDuration = sDuration;
        m_segDuration = segDuration;
        m_minSegs = minSegsForPlayback;
        m_oldCacheSize = oldCacheSize;
        m_playbackBufferSize = playbackBufferSize;
    }

    ApplicationContainer Install(NodeContainer nodes) {
        ApplicationContainer res;
        for(NodeContainer::Iterator n = nodes.Begin(); n != nodes.End(); ++n) {
            Ptr<VideoPlayer> app = CreateObject<VideoPlayer>(m_source, m_oldCacheSize,
                m_playbackBufferSize, m_sDuration, m_segDuration, m_minSegs);

            (*n)->AddApplication(app);
            app->Setup();

            res.Add(app);
        }

        return res;
    }

    // Install on single node
    Ptr<VideoPlayer> Install(Ptr<Node> node) {
        NS_LOG_DEBUG("Install called");
        Ptr<VideoPlayer> app = CreateObject<VideoPlayer>(m_source, m_oldCacheSize,
                m_playbackBufferSize, m_sDuration, m_segDuration, m_minSegs);

        node->AddApplication(app);
        app->Setup();

        return app;
    }

};



///////////////////////////////////////////////////////////////////////////////////////////////////



// // Testing point to point topology
// Ptr<PeerConnector> client;
// Ptr<PeerListener> server;

// void handleHello(MsgType, uint32_t attr, Ptr<MsgSocket> mSock) {
//     std::cout << "Received Hello from other side :)\n";
//     std::cout << "Attribute: " << attr << "\n";

//     NS_LOG_DEBUG("Recvd Hello form client: " << Simulator::Now().GetSeconds());

//     // send back a hello
//     mSock->SendMessage(MSG_HELLO, 2002);
// }

// void handleHello1(MsgType, uint32_t attr, Ptr<MsgSocket>) {
//     std::cout << "Received Hello from server :)\n";
//     std::cout << "Attribute: " << attr << "\n";
//     NS_LOG_DEBUG("Recvd Hello from server: " << Simulator::Now().GetSeconds());
// }


Ptr<VideoSource> vsource;
Ptr<VideoPlayer> vplayer[NUM_VIEWERS];

Ipv4InterfaceContainer interfaces[NUM_VIEWERS + 1];

void checkConnectivity() {
    std::cout << "checkConnectivity called\n";
    // check connectivity of topology
    bool res = true;
    for(int i = 0; i < NUM_VIEWERS; i++) {
        res = (res && (vplayer[i]->NumPeers() == NUM_VIEWERS));
    }
    res = (res && (vsource->NumPeers() == NUM_VIEWERS));

    if(res) {
        std::cout << "Topology connected and looks good :)\n"; 
    }
    else {
        std::cout << "Topology doesn't seem connected yet :(\n";
        for(int i = 0; i < NUM_VIEWERS; i++) {
            std::cout << vplayer[i]->NumPeers() << "\n";
        }
        std::cout << vsource->NumPeers() << "\n";
        // std::cout << vsource->NumPeers() << "\n";
        // std::cout << vplayer->NumPeers() << "\n";
    }

    // // Register hello handler on server & client
    // server->GetPeer(0)->RegisterHandler(MSG_HELLO, 
    //         MakeCallback(&handleHello));

    // client->GetPeer(0)->RegisterHandler(MSG_HELLO, 
    //         MakeCallback(&handleHello1));

    // // Send hello from client to server
    // client->GetPeer(0)->SendMessage(MSG_HELLO, 1995);
    // client->GetPeer(0)->SendMessage(MSG_HELLO, 1996);

}

void connectTopology(const Address & servAddress) {
    NS_LOG_DEBUG(servAddress);
    for(int i = 0; i < NUM_VIEWERS; i++) {
        vplayer[i]->ConnectPeer(servAddress);    
    }

    // Create P2P connections
    for(int i = 0; i < NUM_VIEWERS; i++) {
        for(int j = 0; j < NUM_VIEWERS; j++) {
            if(i != j) {
                NS_LOG_DEBUG("ConnectPeer called");
                vplayer[i]->ConnectPeer(InetSocketAddress(interfaces[j].GetAddress(0), 3434), 
                    vplayer[j]);
            }
        }
    }
      
}


int main(int argc, char *argv[])
{
    initGlobals();

    NodeContainer nodes;
    nodes.Create(NUM_VIEWERS + 2);

    Ptr<Node> router = nodes.Get(NUM_VIEWERS);
    Ptr<Node> server = nodes.Get(NUM_VIEWERS + 1);

    PointToPointHelper ptp;
    ptp.SetDeviceAttribute("DataRate", StringValue("1Mbps"));
    ptp.SetChannelAttribute("Delay", StringValue("2ms"));


    NetDeviceContainer devices[NUM_VIEWERS+1];
    for(int i = 0 ; i < NUM_VIEWERS; i++) {
        devices[i] = ptp.Install(nodes.Get(i), router);
    }
    
    
    ptp.SetDeviceAttribute("DataRate", StringValue("600Kbps"));
    devices[NUM_VIEWERS] = ptp.Install(server, router);

    NS_LOG_DEBUG("Devices installed");

    InternetStackHelper stack;
    stack.Install(nodes);

    NS_LOG_DEBUG("Internet stacks installed");

    Ipv4AddressHelper address;
    

    
    for(int i = 0; i < NUM_VIEWERS + 1; i++) {
        std::stringstream ss;
        ss << "10.1." << i << ".0";
        std::string ipStr;
        ss >> ipStr;

        address.SetBase(ipStr.c_str(), "255.255.255.0");
        interfaces[i] = address.Assign(devices[i]);
    }

    NS_LOG_DEBUG("IP addresses assigned");

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    NS_LOG_DEBUG("Routing tables populated");

    // Create VideoSource and VideoPlayer apps
    // uint16_t chunkRate = (uint16_t)((STREAMING_RATE*SOURCE_DURATION)/MSG_SEG_SIZE);
    uint16_t sourceWindowSize = (uint16_t)(SOURCE_WINDOW_DURATION/SOURCE_DURATION);
    uint16_t playbackBufferSize = (uint16_t)(PLAYBACK_BUFFER_DURATION/SOURCE_DURATION);
    uint16_t oldCacheSize = (uint16_t)(OLDCACHE_DURATION/SOURCE_DURATION);
    vsource = VideoSource::CreateVideoSource(3434, BASE_SEG_NUMBER, 
        Seconds(SOURCE_DURATION), sourceWindowSize, 1 ,server);

    uint16_t minSegsForPlayback = (uint16_t)(MIN_PLAYBACK_START_DURATION/SOURCE_DURATION);
    VideoPlayerHelper vphelper(vsource, playbackBufferSize, oldCacheSize, 
        Seconds(SCHEDULE_DURATION), Seconds(SOURCE_DURATION), minSegsForPlayback);
    for(int i = 0; i < NUM_VIEWERS; i++) {
        vplayer[i] = vphelper.Install(nodes.Get(i));
        vplayer[i]->SetStartTime(Seconds(200.0));
    }
    

    vsource->SetStartTime(Seconds(100.0));

    NS_LOG_DEBUG("Applications created");
    


    // client.ConnectPeer(InetSocketAddress(interfaces.GetAddress(1), 3434));
    // 
    
    // std::cout << interfaces[3].GetAddress(1) << "\n";

    Simulator::Schedule(Seconds(10.0), &connectTopology, InetSocketAddress(interfaces[NUM_VIEWERS].GetAddress(0), 3434));
    Simulator::Schedule(Seconds(20.0), &checkConnectivity);
    Simulator::Stop(Seconds(500.0));
    Simulator::Run();
    Simulator::Destroy();

    std::cout << "Simulation Complete\n";
    
    return 0;
}