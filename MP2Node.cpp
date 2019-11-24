/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
static int debug = 0;
/*Custom log redirector*/
#define printLog(fmt,...)  if(debug) log->LOG(&(memberNode->addr),fmt,__VA_ARGS__);


/* Definitons of the custom message wrapper */
string CustomMessage::stripMyHeader(string message){ 
    /*Strip the custom header from the message and return the rest */
    int pos = message.find(MSG_TAG);
    return message.substr(pos+1);
}
CustomMessage::CustomMessage(string message):Message(CustomMessage::stripMyHeader(message)){
    int  header = stoi(message.substr(0,message.find(MSG_TAG)));
    msgType = static_cast<CustomMessageType>(header);
}
CustomMessage::CustomMessage(CustomMessage::CustomMessageType mt, string normalMsg):Message(normalMsg),msgType(mt){
}
CustomMessage::CustomMessage(CustomMessage::CustomMessageType mt, Message normalMsg):Message(normalMsg),msgType(mt){
}
string CustomMessage::toString(){
    return to_string(msgType) + MSG_TAG + Message::toString();
}


/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
    this->isInitiated = false;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> currentMembers;
	bool change = false;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	currentMembers = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(currentMembers.begin(), currentMembers.end());

    ring = currentMembers; 
    /*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
    stabilizationProtocol(); 
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    g_transID++;
    vector<Node> recipients = findNodes(key);
    for (int i=0;i<NUMBER_OF_KEY_REPLICAS;++i){
        CustomMessage createMsg(CustomMessage::READ_TYPE,Message(g_transID,this->memberNode->addr,MessageType::CREATE,key,value,static_cast<ReplicaType>(i)));
        unicastMsg(createMsg, recipients[i].nodeAddress);
    }
    transaction tr(g_transID,par->getcurrtime(),QUORUM_COUNT,MessageType::CREATE,key,value);
    translog.push_front(tr);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    g_transID++;
    CustomMessage createMsg(CustomMessage::READ_TYPE,Message(g_transID,this->memberNode->addr,MessageType::READ,key));
    /*Store the transaction ID in the list*/
    transaction tr(g_transID,par->getcurrtime(),QUORUM_COUNT,MessageType::READ,key,"");
    translog.push_front(tr);
    vector<Node> recipients = findNodes(key);
    multicastMsg(createMsg,recipients);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
    g_transID++;
    vector<Node> recipients = findNodes(key);
    for (int i=0;i<NUMBER_OF_KEY_REPLICAS;++i){
        CustomMessage createMsg(CustomMessage::READ_TYPE,Message(g_transID,this->memberNode->addr,MessageType::UPDATE,key,value,static_cast<ReplicaType>(i)));
        unicastMsg(createMsg,recipients[i].nodeAddress);
    }
    /*Store the transaction ID in your list*/
    transaction tr(g_transID,par->getcurrtime(),QUORUM_COUNT,MessageType::UPDATE,key,value);
    translog.push_front(tr);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
    if (key=="invalidKey") {
        printLog("Got and invalid key client %s","key");
    }
    g_transID++;
    CustomMessage createMsg(CustomMessage::READ_TYPE,Message(g_transID,this->memberNode->addr,MessageType::DELETE,key));
    /*Store the transaction ID in the list*/
    transaction tr(g_transID,par->getcurrtime(),QUORUM_COUNT,MessageType::DELETE,key,"");
    translog.push_front(tr);
    vector<Node> recipients = findNodes(key);
    multicastMsg(createMsg,recipients);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    Entry entry(value,par->getcurrtime(),replica);
    keymap.insert(pair<string,Entry>(key,entry));
    return true;
    
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
    KeyMap::iterator it;
    if ((it=keymap.find(key))!=keymap.end()) {
        return it->second.convertToString();  
    }
    else return "";
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    Entry e(value,par->getcurrtime(),replica);
    KeyMap::iterator it;
    if ((it=keymap.find(key))!=keymap.end()){
        keymap.insert(pair<string,Entry>(key,e));
        return true;
    }else {
    	return false;
    }
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
    if(keymap.erase(key)) {
    	return true;
    }
    else {
    	return false;
    }
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
        CustomMessage msg(message);
        dispatchMessages(msg);

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/*
 * Called everytime in the recvLoop to decrement transaction timers
 *
 */
void MP2Node::updateTransactionLog(){

    list<transaction>::iterator it=translog.begin();
    while(it!=translog.end())
        if((par->getcurrtime()-it->localTimestamp)>RESPONSE_WAIT_TIME) {
                int transid = it->globaltransId;
                printLog("Transaction Id:",transid);
                switch(it->transactionType){
                    case MessageType::CREATE: log->logCreateFail(&memberNode->addr,true,transid,it->key,it->latestValue.second);break;
                    case MessageType::UPDATE: log->logUpdateFail(&memberNode->addr,true,transid,it->key,it->latestValue.second);break;
                    case MessageType::READ: log->logReadFail(&memberNode->addr,true,transid,it->key);break;
                    case MessageType::DELETE: log->logDeleteFail(&memberNode->addr,true,transid,it->key);break;
                    case MessageType::READREPLY:
                    case MessageType::REPLY:
                    default: break;
                }
                translog.erase(it++);
        }else {
        	it++;
        }
    
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
        updateTransactionLog();
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/* Handles the message type and sends it to the appropriate handler */
void MP2Node::dispatchMessages(CustomMessage message){
	printLog("dispatchMessages %s",message.toString().c_str());

    if(message.msgType==CustomMessage::READ_TYPE){
        switch(message.type){
            /* server side message types */
            case MessageType::CREATE: createKey(message);break;
            case MessageType::UPDATE: updateKey(message);break;
            case MessageType::DELETE: deleteKey(message);break;
            case MessageType::READ: readKey(message);break;
            case MessageType::READREPLY: readReply(message);break;
            case MessageType::REPLY: reply(message);break;
        }
    } else if(message.msgType==CustomMessage::UPDATE_TYPE){
    	updateReplica(message);
    } else{
    	printLog("Dropping corrupted packet %s",message.toString().c_str());
        return;  
    }
}
/*
 * Replication process
 *
 */
void MP2Node::replicate(Node toNode, ReplicaType repType){
	printLog("replicate %s", toNode.nodeAddress.getAddress().c_str());
    KeyMap::iterator it;
    for(it=keymap.begin();it!=keymap.end();++it){
        if(it->second.replica==repType){
            CustomMessage keyupdate(CustomMessage::UPDATE_TYPE,Message(-1,(memberNode->addr),MessageType::CREATE,it->first,it->second.value,ReplicaType::TERTIARY));
            unicastMsg(keyupdate,toNode.nodeAddress);
        }
    }
}
void MP2Node::updateReplica(Message message){
	printLog("Update replica %s",message.toString().c_str());
    createKeyValue(message.key,message.value,message.replica);
}
void MP2Node::createKey(Message message){
    CustomMessage reply(CustomMessage::READ_TYPE,Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false)); 
    if(createKeyValue(message.key,message.value,message.replica)){
        reply.success=true;
        printLog("create server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logCreateSuccess(&memberNode->addr,false,message.transID,message.key,message.value);
    }else{
        reply.success=false;
        log->logCreateFail(&memberNode->addr,false,message.transID,message.key,message.value);
    }
    unicastMsg(reply,message.fromAddr);
}
void MP2Node::updateKey(Message message){
    CustomMessage reply(CustomMessage::READ_TYPE,Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false)); 
    if(updateKeyValue(message.key,message.value,message.replica)){
        reply.success=true;
        printLog("update server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logUpdateSuccess(&memberNode->addr,false,message.transID,message.key,message.value);
    }else{
        reply.success=false;
        log->logUpdateFail(&memberNode->addr,false,message.transID,message.key,message.value);
    }
    unicastMsg(reply,message.fromAddr);
}
void MP2Node::deleteKey(Message message){
    CustomMessage reply(CustomMessage::READ_TYPE,Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false)); 
    if(deletekey(message.key)){
        reply.success=true;
        printLog("delete server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logDeleteSuccess(&memberNode->addr,false,message.transID,message.key);
    }
    else{
        reply.success=false;
        log->logDeleteFail(&memberNode->addr,false,message.transID,message.key);
    }
    unicastMsg(reply,message.fromAddr);      

}
/*
 *
 * The Key read message format does not have a separate flag for success
 *
 */
void MP2Node::readKey(Message message){
    string keyval = readKey(message.key);
    if(!keyval.empty()){
        printLog("read server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logReadSuccess(&memberNode->addr,false,message.transID,message.key,keyval);
    }else{
        log->logReadFail(&memberNode->addr,false,message.transID,message.key);
    }
    CustomMessage reply(CustomMessage::READ_TYPE,Message(message.transID,(this->memberNode->addr),keyval)); 
    unicastMsg(reply,message.fromAddr);
}

void MP2Node::readReply(Message message){
    if(message.value.empty()) {
    	return;
    }
    string delim = ":";
    vector<string> tuple;
    int start = 0;
    int pos = 0;
    while((pos=message.value.find(delim,start))!=string::npos){
        string token = message.value.substr(start,pos-start);
        tuple.push_back(token);
        start = pos+1;
    }
    tuple.push_back(message.value.substr(start));
    assert(tuple.size()==3);
    string keyval = tuple[0];
    int timestamp = stoi(tuple[1]);
    list<transaction>::iterator i;
    for (i = translog.begin(); i!=translog.end();++i) {
        if(i->globaltransId==message.transID){
            break;
        }
        else {
        	printLog("unmatched transid %d",i->globaltransId);
        }
    }
    if(i==translog.end()){
        printLog("dropping reply for transid: %d",message.transID);
    }else if(--(i->quorumCount)==0){
        log->logReadSuccess(&memberNode->addr,true,message.transID,i->key,i->latestValue.second);
        //delete from translog
        translog.erase(i);
    }else{
        if(timestamp>=i->latestValue.first){
            i->latestValue = pair<int,string>(timestamp,keyval);
            printLog("Changing latest val for transid :%d and key: %s",i->globaltransId,i->key.c_str());
        }
    }

}
/*
 * reply received in response to create, read and update messages
 *
 */
void MP2Node::reply(Message message){
    int transid = message.transID;
    list<transaction>::iterator i;
    for (i = translog.begin(); i!=translog.end();++i)
        if(i->globaltransId==transid)
            break;
		if(i==translog.end()) {
			printLog("end of the transaction, reply transid: %d",transid);
		}else if(!message.success){
			printLog("do we have success message??, reply transid: %d",transid);
		}else if(--(i->quorumCount)==0){
			switch(i->transactionType){
				case MessageType::CREATE: log->logCreateSuccess(&memberNode->addr,true,message.transID,i->key,i->latestValue.second);break;
				case MessageType::UPDATE: log->logUpdateSuccess(&memberNode->addr,true,message.transID,i->key,i->latestValue.second);break;
				case MessageType::DELETE: log->logDeleteSuccess(&memberNode->addr,true,message.transID,i->key);break;
        }
        //delete translog
        translog.erase(i);
    }
}


/*
 * multicast message operation on all nodes
 */
void MP2Node::multicastMsg(CustomMessage message,vector<Node>& recipients){

    Address* sendaddr = &(this->memberNode->addr);
    string strrep = message.toString();
    size_t msglen = strlen((char*)strrep.c_str());
    for (size_t i=0;i<recipients.size();++i) {
        this->emulNet->ENsend(sendaddr,&(recipients[i].nodeAddress),(char*)strrep.c_str(),msglen);
    }
}
/*
 *
 * unicast message
 */
void MP2Node::unicastMsg(CustomMessage message,Address& toaddr){
    Address* sendaddr = &(this->memberNode->addr);
    string strrep = message.toString();
    size_t msglen = strlen((char*)strrep.c_str());
    this->emulNet->ENsend(sendaddr,&toaddr,(char*)strrep.c_str(),msglen);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
    int i=0;
    for (i=0;i<ring.size();++i){
        if((ring[i].nodeAddress==this->memberNode->addr)) {
        	break;
        }
    }
    
    /* Initialize neighbours*/
    int positive1 = 0 ;
    if(i-1 < 0) {
        positive1 = (ring.size() + (i-1))%ring.size();
    } else {
        positive1 = (i-1)%ring.size();
    }
    int positive2 = 0;
    if(i-2 < 0) {
    	positive2 = (ring.size() + (i-2))%ring.size();
    } else {
    	positive2 = (i-2)%ring.size();
    }

    int negetive1 = (i+1)%ring.size();
    int negetive2 = (i+2)%ring.size();
    if(!isInitiated){
        haveReplicasOf.push_back(ring[positive2]);
        haveReplicasOf.push_back(ring[positive1]);
        hasMyReplicas.push_back(ring[negetive1]);
        hasMyReplicas.push_back(ring[negetive2]);
        isInitiated = true;
    }else{
        Node n1 = ring[negetive1];
        Node n2 = ring[negetive2];
        Node n3 = ring[positive1];
        if(!(n1.nodeAddress==hasMyReplicas[0].nodeAddress)){
           replicate(n2,ReplicaType::PRIMARY);
        }else if(!(n2.nodeAddress==hasMyReplicas[1].nodeAddress)){
           replicate(n2,ReplicaType::PRIMARY);
        }else if(!(n3.nodeAddress==haveReplicasOf[0].nodeAddress)){
           replicate(n2,ReplicaType::SECONDARY);
        }
        haveReplicasOf.clear();
        hasMyReplicas.clear();
        haveReplicasOf.push_back(ring[positive2]);
        haveReplicasOf.push_back(ring[positive1]);
        hasMyReplicas.push_back(ring[negetive1]);
        hasMyReplicas.push_back(ring[negetive2]);
    }

}
