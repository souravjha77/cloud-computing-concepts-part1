/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <random>
#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        msg = new MessageHdr();
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        JoinReq  joinReq;
        joinReq.id = getId(&memberNode->addr);
        joinReq.port = getPort(& memberNode->addr);
        joinReq.heartbeat = memberNode->heartbeat;
        memcpy(&msg->joinReq, &joinReq, sizeof(JoinReq));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));
        delete msg;
    }
    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    memberNode->bFailed = true;
    memberNode->inGroup = false;
    this->memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilities!
    nodeLoopOps();
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    MessageHdr* messageHdr = (MessageHdr*) data;
    switch (messageHdr->msgType) {
        case JOINREQ:
            return process_join_req(env, messageHdr);
        case JOINREP:
            return process_join_rep(env, messageHdr);
        case HEARTBEAT:
            return process_gossip(env, messageHdr);
        default:
            return false;
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    memberNode->heartbeat += 1;
    checkFailuresAndUpdateMembership();

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

bool MP1Node::process_join_req(void *pVoid,  MessageHdr* messageHdr) {
    upsert_to_membership(messageHdr->joinReq.id, messageHdr->joinReq.port, messageHdr->joinReq.heartbeat);
    send_join_rep(messageHdr);
    return true;
}

bool MP1Node::process_join_rep(void *pVoid, MessageHdr *messageHdr) {
    memberNode->inGroup = true;
    upsert_to_membership(messageHdr->rep.id, messageHdr->rep.port, messageHdr->rep.heartbeat);
    upsert_to_membership(messageHdr->memberList);
    return true;
}

bool MP1Node::process_gossip(void *pVoid, MessageHdr* messageHdr) {
   // cout << "PS\n";
    upsert_to_membership(messageHdr->rep.id, messageHdr->rep.port, messageHdr->rep.heartbeat);
    upsert_to_membership(messageHdr->memberList);
    return true;
}

void MP1Node::upsert_to_membership(int id, short port, long heartbeat) {
     MemberListEntry m(id, port, heartbeat, par->getcurrtime());
     upsert_member(m);
}

void MP1Node::upsert_to_membership(vector<MemberListEntry> members) {
    for (auto member : members) {
            upsert_member(member);
    }
}

bool MP1Node::upsert_member(MemberListEntry entry) {
    if (entry.id > 10 || entry.id <= 0) {
        return false;
    }
    for (auto& member : memberNode->memberList) {
        if (entry.id == member.id  && entry.port  == member.port) {
            member.heartbeat = max(member.heartbeat, entry.heartbeat);
            member.timestamp = max(member.timestamp, entry.timestamp);
            return true;
        }
    }
    log->logNodeAdd(&memberNode->addr, get_address(entry.getid(), entry.getport()));
    memberNode->memberList.push_back(entry);
    return true;
}

Address* MP1Node::get_address(int id, short port) {
    Address* address = new Address();
    memcpy(&address->addr[0], &id, sizeof(int));
    memcpy(&address->addr[4], &port, sizeof(short));
    return address;
}


void MP1Node::send_join_rep(MessageHdr* messageHdr) {
   MessageHdr* msg = new MessageHdr();
   msg->msgType = JOINREP;
   Rep rep{getId(&memberNode->addr), getPort(&memberNode->addr), memberNode->heartbeat};
   memcpy(&msg->rep, &rep, sizeof(Rep));
   msg->memberList = messageHdr->memberList;
   Address* add = get_address(messageHdr->joinReq.id, messageHdr->joinReq.port) ;

   printAddress(add);
   emulNet->ENsend(
           &memberNode->addr,
           add,
           (char*)msg,
           sizeof(MessageHdr));
   delete msg;
}

int MP1Node::getId( Address * adress) {
    int id = 0;
    memcpy(&id, &adress->addr[0], sizeof(int));
    return id;
}

short MP1Node::getPort(Address * address) {
    short port = 0;
    memcpy(&port, &address->addr[4], sizeof(short));
    return port;
}

void MP1Node::checkFailuresAndUpdateMembership() {
    vector<MemberListEntry> new_membershipList;
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        long timestamp = it->timestamp;
        if (par->getcurrtime() - timestamp > TREMOVE) {
            log->logNodeRemove(&memberNode->addr, get_address(it->id, it -> port));

        } else  {new_membershipList.push_back(*it);}
    }
    memberNode->memberList = new_membershipList;
    int num = generate_random(1,6);
    shuffle(
            memberNode->memberList.begin(),
            memberNode->memberList.end(),
            default_random_engine(par->getcurrtime()));
    MessageHdr* msg = new MessageHdr();
    msg->msgType = HEARTBEAT;
    Rep rep;
    rep.id = getId(&memberNode->addr);
    rep.port = getPort(&memberNode->addr);
    rep.heartbeat = memberNode->heartbeat;
    memcpy(&msg->rep, &rep, sizeof(Rep));
    msg->memberList = memberNode->memberList;
    if (num <= 5 && memberNode->memberList.size() > 0){
            int member_no = generate_random(1, memberNode->memberList.size()) - 1;
            emulNet->ENsend(
                    &memberNode->addr,
                    get_address(memberNode->memberList[member_no].id,  memberNode->memberList[member_no].port),
                    (char*)msg,
                    sizeof(MessageHdr));
    }
    delete msg;
}

int MP1Node::generate_random(int lo, int hi) const {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(lo, hi);
    return dis(gen);
}

