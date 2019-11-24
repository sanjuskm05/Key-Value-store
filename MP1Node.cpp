/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

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
	        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
	        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

	        // create JOINREQ message: format of data is {struct Address myaddr}
	        msg->msgType = JOINREQ;
	        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

	#ifdef DEBUGLOG
	        sprintf(s, "Trying to join...");
	        log->LOG(&memberNode->addr, s);
	#endif

	        // send JOINREQ message to introducer member
	        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

	        free(msg);
	    }

	    return 1;


}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	memberNode->memberList.clear();
    return 0;
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

    // ...then jump in and share your responsibilites!
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
 * Update member heartbeat and timestamp in the MemberList table
 */
bool MP1Node::updateMemberInMemberList(int id, short port, long heartbeat) {

    for (vector<MemberListEntry>::iterator member = memberNode->memberList.begin(); member != memberNode->memberList.end(); member++) {
        if (member->id == id && member->port == port) {
            if (heartbeat > member->heartbeat) {
              log->LOG(&memberNode->addr, "\t\tUpdateMember new heartbeat[%li] timestamp[%li] old heartbeat[%li] timestamp[%li]",
                         heartbeat, par->getcurrtime(), member->heartbeat, member->timestamp);

                member->setheartbeat(heartbeat);
                member->settimestamp(par->getcurrtime());
            }
            return true;
        }
    }
    return false;

}

/**
 * Add member entry to MemberList table
 */
bool MP1Node::addMemberInMemberList(int id, short port, long heartbeat) {
	Address newMemberAddr;
	memcpy(&newMemberAddr.addr[0], &id, sizeof(int));
	memcpy(&newMemberAddr.addr[4], &port, sizeof(short));
	log->logNodeAdd(&memberNode->addr, &newMemberAddr);
	MemberListEntry newMember(id, port, heartbeat, par->getcurrtime());
	memberNode->memberList.emplace_back(newMember);

	return true;
}
/**
 * Remove eligible members
 */
long MP1Node::cleanUpMemberList(long totalMembersCount, char *data) {
	for (vector<MemberListEntry>::iterator member = memberNode->memberList.begin();
			member != memberNode->memberList.end();) {
		if (member != memberNode->memberList.begin()) {
			long ageOfMsg = par->getcurrtime() - member->timestamp;
			if (ageOfMsg > TCLEANUP) {
				log->LOG(&memberNode->addr, "Member CLEANUP! id[%i] currenttime[%li] timestamp[%li]", member->id, par->getcurrtime(), member->timestamp);
				// Member CLEANUP!
				Address addrToRemove;
				memcpy(&addrToRemove.addr[0], &member->id, sizeof(int));
				memcpy(&addrToRemove.addr[4], &member->port, sizeof(short));
				log->logNodeRemove(&memberNode->addr, &addrToRemove);
				member = memberNode->memberList.erase(member);
				totalMembersCount--;
				continue;
			} else if (ageOfMsg > TFAIL) {
				log->LOG(&memberNode->addr, "Member FAILED! id[%i] currenttime[%li] timestamp[%li]", member->id, par->getcurrtime(), member->timestamp);
				totalMembersCount--;
				member++;
				continue;
			}
		}
		log->LOG(&memberNode->addr, "Member %i:%i: heartbeat[%li] timestamp[%li]", member->id, member->port, member->heartbeat, member->timestamp);
		memcpy(data, &member->id, sizeof(int));
		data += sizeof(int);
		memcpy(data , &member->port, sizeof(short));
		data += sizeof(short);
		memcpy(data , &member->heartbeat, sizeof(long));
		data += sizeof(long);

		member++;
	}
	return totalMembersCount;
}
/**
 * Propagate message to other member
 */
void MP1Node::disseminateMsg(const char * label, enum MsgTypes msgType, Address * toAddr)
{
    log->LOG(&memberNode->addr, "Message %s sending to: %s", label, toAddr->getAddress().c_str());
    long totalMembersCount = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + totalMembersCount * (sizeof(int) + sizeof(short) + sizeof(log));

    MessageHdr * msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    char * data = (char*)(msg + 1);

    msg->msgType = msgType;
    //TODO fix it
    memcpy(data, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    data += sizeof(memberNode->addr.addr);
    char* currentMemberPosition = data;
    data += sizeof(long);
	totalMembersCount = cleanUpMemberList(totalMembersCount, data);
	log->LOG(&memberNode->addr, "Total Members count in the list %d", totalMembersCount);
	//TODO fix it
    memcpy(currentMemberPosition, &totalMembersCount, sizeof(long));
    msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + totalMembersCount * (sizeof(int) + sizeof(short) + sizeof(log));

    //Send message
    emulNet->ENsend(&memberNode->addr, toAddr, (char *)msg, msgsize);
    free(msg);

    log->LOG(&memberNode->addr, "Message %s sent to: %s", label, toAddr->getAddress().c_str());
}


/**
 * Refresh MemberList table
 */
bool MP1Node::refreshMemberList(const char * label, void *env, char *data, int size) {
    if (size < (int)(sizeof(long))) {
    	log->LOG(&memberNode->addr, "Message %s received with "
    			"size less than (long size) %d. Ignored.", label, (int)(sizeof(long)));
    	return false;
    }

    long totalMemberCount;
    memcpy(&totalMemberCount, data, sizeof(long));
    data += sizeof(long);

    if (size < (int)(totalMemberCount * (sizeof(int) + sizeof(short) + sizeof(log)))) {
        log->LOG(&memberNode->addr, "Message %s received with wrong size. Ignored.", label);
        return false;
    }

    MemberListEntry member;
    for (long i = 0; i < totalMemberCount; i++) {
        memcpy(&member.id, data, sizeof(int));
        data += sizeof(int);
        memcpy(&member.port, data, sizeof(short));
        data += sizeof(short);
        memcpy(&member.heartbeat, data, sizeof(long));
        data += sizeof(long);
        member.timestamp = par->getcurrtime();
        //Refresh the member entry if exist or add to the table
        if(!updateMemberInMemberList(member.getid(), member.getport(), member.getheartbeat())) {
            addMemberInMemberList(member.getid(), member.getport(), member.getheartbeat());
        }
    }
    return true;
}
/**
 * Send join request for the members
 *
 */
bool MP1Node::sendJoinReplyMsg(void *env, char *data) {

    Address memberJoinAddress;
    long heartbeat;

    memcpy(memberJoinAddress.addr, data, sizeof(memberNode->addr.addr));
    memcpy(&heartbeat, data + sizeof(memberNode->addr.addr), sizeof(long));

    log->LOG(&memberNode->addr, "Message JOINREQ sent to %s", memberJoinAddress.getAddress().c_str());
    int id = *(int*)(&memberJoinAddress.addr[0]);
    int port = *(short*)(&memberJoinAddress.addr[4]);
    log->LOG(&memberNode->addr, "Message JOINREQ heartbeat : %li", heartbeat);

    if(!updateMemberInMemberList(id, port, heartbeat)) {
    	addMemberInMemberList(id, port, heartbeat);
    }

    disseminateMsg("JOINREP", JOINREP, &memberJoinAddress);
    return true;
}
/**
 * Receive Join response message
 */
bool MP1Node::recvJoinReplyMsg(void *env, char *data, int size) {

	Address fromAddr;
    memcpy(fromAddr.addr, data, sizeof(memberNode->addr.addr));
    data += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);
    log->LOG(&memberNode->addr, "Message JOINREP received from %s", fromAddr.getAddress().c_str());


    if (!refreshMemberList("JOINREP", env, data, size)) {
        return false;
    }
    memberNode->inGroup = true;
    return true;
}

/**
 * Send Heart beat message to associated Member
 */
bool MP1Node::sendHeartBeatMsg(void *env, char *data, int size) {

    Address toAddr;
    memcpy(toAddr.addr, data, sizeof(memberNode->addr.addr));
    data += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);

    if (!refreshMemberList("SENDHEARTBEAT", env, data, size)) {
        return false;
    }

    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr * msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = RECEIVEHEARTBEAT;
    memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));

    emulNet->ENsend(&memberNode->addr, &toAddr, (char *)msg, msgsize);
    free(msg);
    return true;
}
/**
 * Update heartbeat after receiving the heartbeat msg
 */
bool MP1Node::recvHeartbeatMsg(void *env, char *data, int size) {

    Address fromAddr;
    memcpy(fromAddr.addr, data, sizeof(memberNode->addr.addr));

    int id = *(int*)(&fromAddr.addr[0]);
    int port = *(short*)(&fromAddr.addr[4]);

    //Increment the heartbeat of MemberList table entries by 1
    for (vector<MemberListEntry>::iterator member = memberNode->memberList.begin();
    		member != memberNode->memberList.end(); ++member) {
        if (member->id == id && member->port == port) {
            member->heartbeat++;
            member->timestamp = par->getcurrtime();
            log->LOG(&memberNode->addr, "Incremented heartbeat of Member %i:%i: "
            		"to heartbeat[%li] timestamp[%li]",
					(*member).id, (*member).port, (*member).heartbeat, (*member).timestamp);
            return true;
        }
    }
    return false;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    log->LOG(&memberNode->addr, "Message recvCallBack, size :%d", size);

    // If member not in the group then accept only JOINREP msg type
    if ( (memberNode->inGroup && ((MessageHdr *)data)->msgType >= 0)
    			|| (!memberNode->inGroup && ((MessageHdr *)data)->msgType == JOINREP) ) {

		if(((MessageHdr *)data)->msgType == JOINREQ) {
			return sendJoinReplyMsg(env, data + sizeof(MessageHdr));
		} else if (((MessageHdr *)data)->msgType == JOINREP) {
			return recvJoinReplyMsg(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
		} else if(((MessageHdr *)data)->msgType == SENDHEARTBEAT) {
			return sendHeartBeatMsg(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
		} else if(((MessageHdr *)data)->msgType == RECEIVEHEARTBEAT) {
			return recvHeartbeatMsg(env, data + sizeof(MessageHdr), size - sizeof(MessageHdr));
		}
    }
    free(data);
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	memberNode->memberList.begin()->heartbeat++;
	memberNode->memberList.begin()->timestamp = par->getcurrtime();

	int randPositionInMemberList = rand() % memberNode->memberList.size();
	MemberListEntry &member = memberNode->memberList[randPositionInMemberList];

	if (par->getcurrtime() - member.timestamp > TFAIL) {
		//Do nothing when age of the member is less than TFail timestamp
		return;
	}

	Address memberAddr;
	memcpy(&memberAddr.addr[0], &member.id, sizeof(int));
	memcpy(&memberAddr.addr[4], &member.port, sizeof(short));
	log->LOG(&memberNode->addr, "Random Member in nodeLoops! id[%i] port[%i]", member.id, member.port);

	disseminateMsg("SENDHEARTBEAT", SENDHEARTBEAT, &memberAddr);
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

    MemberListEntry member(memberNode->addr.addr[0],
    		memberNode->addr.addr[4], 0, par->getcurrtime());
    memberNode->memberList.emplace_back(member);
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
