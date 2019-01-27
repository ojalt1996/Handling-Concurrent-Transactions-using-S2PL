/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "../include/zgt_def.h"
#include "../include/zgt_tm.h"
#include "../include/zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //starts opeartion by doing conditional wait
extern void *finish_operation(long);       // finishes abn operation by removing conditional wait
extern void *open_logfile_for_append();    //opens log file for writing
extern void *do_commit_abort(long, char);   //commit/abort based on char value (the code is same for us)
//extern void *perform_readWrite(long ,long, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

FILE *logfile; //declare globally to be used by all

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2014[jay] R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //Initialize a transaction object. Make sure it is
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
    struct param *node = (struct param*)arg;// get tid and count
    start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
    open_logfile_for_append();
    fprintf(logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(logfile);
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit

}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
int db;
  struct param *node = (struct param*)arg;// get tid and objno and count
  zgt_hlink *cNode;
   // Used at the begining to make sure all the operations under the transaction are in a serial manner
  start_operation(node->tid, node->count);

  //the thread will wait for the previous transactions to complete then the below code will be executed
  //Now we are locking the manager
  zgt_p(0);
  //used to get information about the status of the transaction
  zgt_tx *currentTransaction = get_tx(node->tid);
  char currentTransactionStatus = currentTransaction->get_status();

  //We check wether it is in the P state or the W state
  //If it is in the P state then we lock 
    
    //Checking wether the object is present in the buckets of the hash table inorder to find which lock it has
    cNode = ZGT_Ht->find(currentTransaction->sgno, node->obno);
    //retrieving the Lock Mode
    if(cNode!=NULL)
     { 
    if(cNode->lockmode == 'X')
    { 

    currentTransaction->set_lock(node->tid, 1, node->obno, node->count, 'S');
    //now we finish the operation by broadcasting and exiting the thread
    finish_operation(node->tid);
    pthread_exit(NULL);

    }
  	else
    {
      //Releasing the transaction manager lock
      zgt_v(0);
      //performing the read write operations
      currentTransaction->perform_readWrite(node->tid, node->obno, 'S');
    
      //now we finish the operation by broadcasting and exiting the thread
      finish_operation(node->tid);
      pthread_exit(NULL); 
      return (0);
    }

  }
  else
  {

	  db = ZGT_Ht->add(currentTransaction, currentTransaction->sgno, node->obno, 'S');
	  //Releasing the transaction manager lock
      zgt_v(0);
      //performing the read write operations
      currentTransaction->perform_readWrite(node->tid, node->obno, 'S');
    
      //now we finish the operation by broadcasting and exiting the thread
      finish_operation(node->tid);
      pthread_exit(NULL); 
      return (0);

  }

}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
	struct param *node = (struct param*)arg;	// struct parameter that contains
  

  // Used at the begining to make sure all the operations under the transaction are in a serial manner
	start_operation(node->tid, node->count);

  //the thread will wait for the previous transactions to complete then the below code will be executed
  //Now we are locking the manager
	zgt_p(0);
	//used to get information about the status of the transaction
	zgt_tx *currentTransaction = get_tx(node->tid);
	char currentTransactionStatus = currentTransaction->get_status();

	//We check wether it is in the P state or the W state
	//If it is in the P state then we lock 

		//now we call set_loc() to lock in exclusive mode and make an entry
		currentTransaction->set_lock(node->tid, 1, node->obno, node->count, 'X');
		//now we finish the operation by broadcasting and exiting the thread
		finish_operation(node->tid);
		pthread_exit(NULL);
		return (0);
}

void *aborttx(void *arg)
{
  zgt_tx *currentTransactionPointer;
  struct param *node = (struct param*)arg;// get tid and count  

  open_logfile_for_append();
  //Making sure that all the transactions before this are completed
  start_operation(node->tid, node->count);
  //Locking the transaction manager
  zgt_p(0);
  //Getting the current transaction pointer
  currentTransactionPointer = get_tx(node->tid);

  //Checking wether the transaction exists or not
  if(currentTransactionPointer != NULL)
  {
    //Entering that into the log file
    fprintf(logfile,"T%d\t\tAbortTx\t\n",node->tid);
    //performing the abort on the transaction and setting the status accordingly
    fflush(logfile);
    do_commit_abort(node->tid, 'A');
  }
  else
  {
    //Entering that the transaction is not present in the log file
    fprintf(logfile, "\n Transaction doesnt exist \n");
  }

  //Releasing the transaction manager lock
  zgt_v(0);
  //Finishing the operation
  finish_operation(node->tid);
  //Exiting the thread
  pthread_exit(NULL);
}

void *committx(void *arg)
{
  zgt_tx *currentTransactionPointer;
  struct param *node = (struct param*)arg;// get tid and count  

  open_logfile_for_append();
  //Making sure that all the transactions before this are completed
  start_operation(node->tid, node->count);
  //Locking the transaction manager
  zgt_p(0);
  //Getting the current transaction pointer
  currentTransactionPointer = get_tx(node->tid);

  //Checking wether the transaction exists or not
  if(currentTransactionPointer != NULL)
  {
    //Entering that into the log file
    fprintf(logfile,"T%d\t\tCommitTx\t\n",node->tid);
    //performing the abort on the transaction and setting the status accordingly
    fflush(logfile);
    do_commit_abort(node->tid, 'V');
  }
  else
  {
    //Entering that the transaction is not present in the log file
    fprintf(logfile, "\n Transaction doesnt exist \n");
  }

  //Releasing the transaction manager lock
  zgt_v(0);
  //Finishing the operation
  finish_operation(node->tid);
  //Exiting the thread
  pthread_exit(NULL);
}

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existant tx

void *do_commit_abort(long t, char status){

  int semnumber,wTx,i;
  open_logfile_for_append();
  //Getting the current transaction pointer
  zgt_tx *currentTransactionPointer = get_tx(t);
  //Updating the status of the transaction to abort state
  currentTransactionPointer->status = status;
  //Releasing all locks that were held by the transaction
  currentTransactionPointer->free_locks();

  //Retrieving the semaphore number of the process
  semnumber = currentTransactionPointer->semno;
  //Removing the transaction entry
  currentTransactionPointer->remove_tx();

  //Checking if someone is waiting in this semaphore
  if(semnumber > -1)
  {
      wTx = zgt_nwait(currentTransactionPointer->semno);
      if(wTx > 0)
      {
        for(i=wTx;i>0;i--)
        {
          //Releasing all the locks
          zgt_v(currentTransactionPointer -> semno);
        }
      }
  }



}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if req node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx in this*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){

	zgt_hlink *currentNode, *currentOwner;
	int transactionStatus;

	open_logfile_for_append();		

	//We check wether the operation is from the same transaction or a different transaction (owner)
	currentOwner = ZGT_Ht->findt(this->tid, sgno1, obno1);

	//ZGT_Ht returns a not null value if it belongs to the same transaction hence a lock can be granted
	if(currentOwner != NULL)
	{
		//Changing the status to active
		status = TR_ACTIVE;
		//setting the lock mode which is passed
		currentOwner->lockmode = lockmode1;
		//Releasing the transaction manager lock
		zgt_v(0);
		//performing the read or write operation						
    this->perform_readWrite(tid, obno1, lockmode1);
	}								

	//Now if the operation doesnt belong to the same transaction then the else code is executed
	else
	{
		//Checking wether the object is present in the buckets of the hash table
		currentNode = ZGT_Ht->find(sgno1, obno1);
		cout << "set_lock method";
		cout << obno1;
		//If it returns null it means it is not in the hash table and hense there is no lock on it 
		if(currentNode == NULL)
		{	//Adding objct to the hash table
			transactionStatus = ZGT_Ht->add(this, sgno1, obno1, lockmode1);
			//To check wether it has been succefully added or not
			if (transactionStatus >= 0) 
			{
				
        //Releasing the transaction manager lock
				zgt_v(0);
				//performing the read write operations
				perform_readWrite(tid1, obno1, lockmode1); 
                return (0);
			}
			//Goes into else if the pbject cannot be added to the hash table	
			else
			{
				//Releasing the transaction manager lock
				zgt_v(0);
				cout<<"Cannot add to the hash table";
			}


		}
		else
		{

		//We are setting the transaction status to W
		this->status = TR_WAIT;						
		//Updating the lock mode				
        this->lockmode = lockmode1;
        //intitializing the object again
        this->obno = obno1;
        //updating the semaphore value
        this->setTx_semno(currentNode->tid,currentNode->tid);
        //releasing the transaction manager object
        zgt_v(0);
        //Waiting for some time and checking agin if the lock can be set
        zgt_p(currentNode->tid);
        cout<<"fuckoff";
        //Setting lock on the manager
        zgt_p(0);
        //Changing the status to active
       	this->status = TR_ACTIVE;
       	//Again calling the set_lock()
        set_lock(this->tid, this->sgno, this->obno, count, this->lockmode);

        return(0);
		}
	}




}

// this part frees all locks owned by the transaction
// Need to free the thread in the waiting queue
// try to obtain the lock for the freed threads
// if the process itself is blocked, clear the wait and semaphores

int zgt_tx::free_locks()
{
  zgt_hlink* temp = head;  //first obj of tx
  
  open_logfile_for_append();
   
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      //fprintf(logfile, "%d : %d \t", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(logfile, "\n");
  fflush(logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2014: not used
{
  zgt_tx *linktx, *prevp;

  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// check which other transaction has the lock on the same obno
// returns the hash node
zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
{
  zgt_hlink *ep;
  ep=ZGT_Ht->find(sgno1,obno1);
  while (ep)				// while ep is not null
    {
      if ((ep->obno == obno1)&&(ep->sgno ==sgno1)&&(ep->tid !=this->tid)) 
	return (ep);			// return the hashnode that holds the lock
      else  ep = ep->next;		
    }					
  return (NULL);			//  Return null otherwise 
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print

void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//currently not used
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}
void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the acutual read/write operation
// based  on the lockmode
void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){

  int i,j,objectValue,computationTime;
  open_logfile_for_append();
  
  objectValue =  ZGT_Sh->objarray[obno]->value;
  computationTime = ZGT_Sh->optime[tid]*2;


  if(lockmode == 'X')
  {
    //Incrementing the value by 1 as it is in write mode
    ZGT_Sh->objarray[obno]->value = objectValue + 1;
    //Here we are printing the values into the log file
    fprintf(logfile, "T%d\t      \tWriteTx\t\t%d:%d:%d\t\tWriteLock\tGranted\t\t %c\n",
            this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
    fflush(logfile);
    //Trying to simulate the operations
    for(i = 0; i < computationTime; i++)
    {
    j=j*i;
    }

  }
  if(lockmode == 'S')
  {
    //Decrementing the value by 1 as it is in read mode
    ZGT_Sh->objarray[obno]->value = objectValue + 1;
    //Here we are printing the values into the log file
    fprintf(logfile, "T%d\t      \tReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t\t %c\n",
            this->tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
    fflush(logfile);
    //Trying to simulate the operations
    for(i = 0; i < computationTime; i++)
    {
    j=j*i;
    }

  }


}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    return(-1);
  }
  if (txptr->semno == -1){
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

// routine to start an operation by checking the previous operation of the same
// tx has completed; otherwise, it will do a conditional wait until the
// current thread signals a broadcast

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of the start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}

void *open_logfile_for_append(){
  
  if ((logfile = fopen(ZGT_Sh->logfile, "a")) == NULL){
    printf("\nCannot open log file for append:%s\n", ZGT_Sh->logfile);
    fflush(stdout);
    exit(1);
  }
}
