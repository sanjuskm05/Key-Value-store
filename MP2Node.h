/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <list>

#define NUMBER_OF_KEY_REPLICAS 3
#define QUORUM_COUNT (((NUMBER_OF_KEY_REPLICAS)/2) + 1)
#define RESPONSE_WAIT_TIME 30
#define MSG_TAG '#'
/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */

/* Custom message wrapper needed for replicate messages */
class CustomMessage:public Message {
    public:
    enum CustomMessageType{ UPDATE_TYPE, READ_TYPE };
    CustomMessage(string msg);
    CustomMessage(CustomMessageType msgType, string normalMsg);
    CustomMessage(CustomMessageType msgType, Message normalMsg);
    string toString();
    static string stripMyHeader(string message); 
    CustomMessageType msgType;
};

/*
 * Custom class implementation for storing transaction
 *
 */
struct transaction {
    public:
    int globaltransId;
    int localTimestamp;
    int quorumCount;
    MessageType transactionType;
    string key;
    pair<int,string> latestValue;
    transaction(int transId, int localTs, int qc, MessageType ttype,string k,string value):
    globaltransId(transId),
    localTimestamp(localTs),
    quorumCount(qc),
    transactionType(ttype),
    key(k),
    latestValue(0,value){
    } 
};
/* custom map */
typedef std::map<string,Entry> KeyMap;
/* Custom class implementations that will be used in MP2Node*/

class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;


    list<transaction> translog;
    KeyMap keymap;


    bool isInitiated;
    /**Server msg*/
    void createKey(Message msg);
    void updateKey(Message msg);
    void deleteKey(Message msg);
    void readKey(Message msg);
    void replicate(Node toNode, ReplicaType rType);
    void updateReplica(Message msg);

    /* client msg */
    void readReply(Message msg);
    void reply(Message msg);

    /* Util functions to propogate messages */
    void unicastMsg(CustomMessage msg, Address& toaddr);
    void multicastMsg(CustomMessage msg,vector<Node>& recp);
    
    /* transaction timeouts*/
    void updateTransactionLog();

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(CustomMessage message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();
};

#endif /* MP2NODE_H_ */
