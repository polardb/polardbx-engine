from requestInfo_pb2 import requestInfoWrapper
from responseInfo_pb2 import responseInfoWrapper
from twisted.internet import reactor
from twisted.internet import reactor,threads
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import ClientCreator
from twisted.internet.protocol import ClientFactory
#from rds.base.lib import log
from threading import Thread
from socket import socket
from time import sleep
import thread
import threading
import Queue
import time
import struct
import uuid





"""
sdk drivermanager
"""
class DriverManager(object):
    """ single instance """
    driverManager = None;
    configDO = None;
    singleInstanceLock = threading.Condition(threading.Lock());
    
    def __init__(self,configDO):
        self.configDO = configDO;
        DataSourceManager.getInstance(configDO);
        

    """
    get DriverManager single instance
    """
    @classmethod
    def getInstance(clazz,configDO = None):
        if clazz.driverManager is None:
            if not configDO is None:
                clazz.singleInstanceLock.acquire();
                try:
                    """double check is necessary"""
                    if clazz.driverManager is None :
                        print("initing driverManager ")
                        clazz.driverManager = DriverManager(configDO);
                        print("inited driverManager ")
                except Exception:
                    pass
                finally:
                    clazz.singleInstanceLock.release();
            else :
                print("configDO shoud not be None when init driverManager .")
        return  clazz.driverManager;

    """
    get client connection
    """
    def getCloudDBAClient(self):
        return CloudDBAClientImpl();
   

    """
    release resource
    """
    @classmethod
    def release(clazz):
        DataSourceManager.release();




"""
sdk clientImpl
"""
class CloudDBAClientImpl(object):
    def __init__(self):
        pass

    """
    synchognized method with server
    waiting for server response
    """
    def doCommand(self, requestInfo):
        """
        if DataSourceManager is not available just return 
        """
        if DataSourceManager.isAvailable() == False:
            raise Exception("sdk is not available, maybe cannot connect server !")
            return None
        
        configDO = DataSourceManager.configDO;
        requestInfo = MessageUtil.setRequestHeader(requestInfo,configDO);
        messageID = requestInfo.getMessageID();
        
        lock = threading.Condition();
            
        """
        about lock.wait
        If the calling thread has not acquired the lock when this method is
        called, a RuntimeError is raised. so as the lock.notify.
        
        when calling lock.notify() we should calling lock.acquire
        so we doesnot worry about lock.notify is excuted before lock.wait as lock doesnot
        is released util lock.wait so lock.notify will not get the lock
        then the code lock.acquire before lock.notify will be blocked
        """
        if lock.acquire():
            try:
                DataSourceManager.lockMap[messageID] = lock; 
                DataSourceManager.sendBuff.put(requestInfo);
                DataSourceManager.sendThreadPool.queueTask(DataSourceManager.sendTask);
                
                """
                put recvTask is moved to twist dataReceived method
                """
                #DataSourceManager.recvThreadPool.queueTask(DataSourceManager.recvTask);
                
                lock.wait(configDO.lockTime);
                """
                normally,lockMap will pop by threadpool
                but we should do pop lockmap when timeout
                """
                DataSourceManager.lockMap.pop(messageID, None);
            
                responseInfo = None;
                responseInfo = DataSourceManager.responseMap.pop(messageID);
            except Exception, error:
                print("doCommand lock time out , messageID is %s" % str(messageID));
            finally:
                lock.release();
            return responseInfo;


class DataSourceManager(object):
    """
    Queue feature  
        1.put(self, item, block=True, timeout=None)
        2.get(self, block=True, timeout=None)
        3.multiple threads safety
    """
    sendBuff = Queue.Queue();
    recvBuff = Queue.Queue();
    
    sendThreadPool = None;
    recvThreadPool = None;
   
    lockMap = dict();
    sendLockMap = dict();
    responseMap = dict();
    configDO = None;
    
    """ 
    socketMap format: messageID/socket
    socketQueueMap format: messageID/socketQuee
    logic is :
        when do sendtask  put messageID to socketMap and socketQueueMap
        when do recvtask  put the socket into queue with messageID
    """
    socketMap = dict();
    socketQueueMap = dict();
    
    dataSourceManager = None;
    """"control single instance """
    singleInstanceLock = threading.Condition(threading.Lock())
    
    def __init__(self, configDO):
        print("begin init DataSourceManager ")
        DataSourceManager.configDO = configDO;
        DataSourceManager.sendThreadPool = FlexThreadPool(configDO.sendThreads, 'FlexThreadPoolSendThread', configDO);
        DataSourceManager.recvThreadPool = FlexThreadPool(configDO.recvThreads, 'FlexThreadPoolRecvThread', configDO);
        """
        note reactor.run is blocking method
        if donnot use a new thread the main thread will block 
        """
        Thread(target=reactor.run, args=(False,)).start()
        print("DataSourceManager started reactor.run")
        print("inited DataSourceManager")
    
    """
    mean whether sdk is available
    in case of connection lost and sendthreadPool exit
        and appThreads are always calling doCommand 
        and just for wait timeout
    """
    @classmethod
    def isAvailable(clazz):
        if not clazz.sendThreadPool is None:
            """
            why donot use clazz.sendThreadPool.getThreadCount() > 0
            at the beginning maybe clazz.sendThreadPool.getThreadCount() == 0
            but available cannot be False except change manually
            """
            return clazz.sendThreadPool.available 
        else:
            """
            If sendThreadPool is not inited finish, sdk is not available 
            """
            return False
        
    """
    exception is catched by send thread
    """
    @classmethod
    def sendTask(clazz, tcpCliSock, socketQueue):
        requestInfo = clazz.sendBuff.get(True, DataSourceManager.configDO.lockTime)
        requestWrapper = MessageUtil.getRequestWrapper(requestInfo)
        """ call protocbuff api to send message """
        request = requestWrapper.SerializeToString().encode(encoding="utf-8")
        requestLen = len(request) + 4;
        requestLenBytes = struct.pack("i", requestLen) 
        """
        requestType
        """
        requestType =  1
        requestTypeBytes = struct.pack("i", requestType) 
        """store data"""
        messageID = requestInfo.getMessageID()
        clazz.socketMap[messageID] = tcpCliSock
        clazz.socketQueueMap[messageID] = socketQueue;
        
        """send data"""
        tcpCliSock.sendMessage(requestLenBytes[::-1] + requestTypeBytes[::-1] + request)

  
    """
    exception is catched by recv thread
    """
    @classmethod
    def recvTask(clazz):
        data = clazz.recvBuff.get(True, DataSourceManager.configDO.lockTime);
     
        responseWrapper = responseInfoWrapper();
        responseWrapper.ParseFromString(data);
        responseInfo = MessageUtil.getResponseInfo(responseWrapper);
        messageID = responseInfo.getMessageID();
        
        """return socket to thread Queue"""
        socket = clazz.socketMap.pop(messageID, None);
        socketQueue = clazz.socketQueueMap.pop(messageID, None);
        if not socket is None and not socketQueue is None:
            socketQueue.put(socket);
    
        """
        notify doComand
        """
        lock = DataSourceManager.lockMap.pop(messageID,None);
        if not lock is None:
            if lock.acquire():
                try:
                    DataSourceManager.responseMap[messageID] = responseInfo;
                    lock.notify();
                finally:
                    """
                    Note: an awakened thread does not actually return from its wait() call
                        until it can reacquire the lock.
                        Since notify() does not release the lock, its caller should.
                    """
                    lock.release();

    @classmethod
    def getInstance(clazz,configDO = None):
        if clazz.dataSourceManager is None:
            if not configDO is None:
                clazz.singleInstanceLock.acquire();
                """double check is necessary"""
                try:
                    if clazz.dataSourceManager is None:
                        clazz.dataSourceManager = DataSourceManager(configDO);
                finally:
                    clazz.singleInstanceLock.release();
            else:
                print("error occurs , dataSourceManager should be inited .");
        return clazz.dataSourceManager;

    @classmethod
    def toString(clazz):
        buff = "dataSourceManager states as follows : \n";
        buff = buff + "lockMap size : " + str(len(DataSourceManager.lockMap)) + "\n";
        buff = buff + "sendBuff size : " + str(DataSourceManager.sendBuff.qsize()) + "\n";
        buff = buff + "sendThreadPool sendthread size : " + str(DataSourceManager.sendThreadPool.getThreadCount()) + "\n";
        buff = buff + "sendThreadPool task size : " + str(DataSourceManager.sendThreadPool.getQueueSize()) + "\n";
        buff = buff + "recvThreadPool recvthread size : " + str(DataSourceManager.recvThreadPool.getThreadCount()) + "\n";
        buff = buff + "recvThreadPool task size : " + str(DataSourceManager.recvThreadPool.getQueueSize()) + "\n";
        return buff;

    @classmethod
    def release(clazz):
        
        if DataSourceManager.isAvailable() == False:
            print("SDK is not available before release, sleep 3s to inited SDK thread pool !");
            time.sleep(3);
        
        if DataSourceManager.isAvailable() == True:
            DataSourceManager.sendThreadPool.joinAll(False, False);
            DataSourceManager.recvThreadPool.joinAll(False , False);
            time.sleep(5);
            
        """
        sockets escape from thread pool and dont go back to the queue
        """
        leakSockets = clazz.socketMap.values();
        print ("**** lockMap leak socket size is %d try to release ****" % len(leakSockets))
        for socket in leakSockets:
            socket.closeConnection();

        
        leakSocketSize = clazz.sendThreadPool.socketFactory.socketSize()
        print ("**** socketFactory leak socket size is %d try to release ****" % leakSocketSize)
        clazz.sendThreadPool.socketFactory.releaseSockets();
        
        print("begin stoping reactor");
        reactor.stop();


class FlexThreadPool:

    """Flexible thread pool class.  Creates a pool of threads, then
    accepts tasks that will be dispatched to the next available
    thread."""
    
    def __init__(self, numThreads, threadName = 'FlexThreadPoolSendThread', configDO = None):

        """Initialize the thread pool with numThreads workers."""
        self.__threads = []
        self.__resizeLock = threading.Condition(threading.Lock())
        self.__taskLock = threading.Condition(threading.Lock())
        self.__tasks = []
        self.__isJoining = False
        self.configDO = configDO;
        self.threadName = threadName
        self.socketFactory = SocketFactory(configDO)
        self.setThreadCount(numThreads)
        """
        whether has thread running
        """
        self.available = True;
       
       

    def setThreadCount(self, newNumThreads):

        """ External method to set the current pool size.  Acquires
        the resizing lock, then calls the internal version to do real
        work."""
        # Can't change the thread count if we're shutting down the pool!
        if self.__isJoining:
            return False
        
        self.__resizeLock.acquire()
        try:
            self.__setThreadCountNolock(newNumThreads)
        finally:           
            self.__resizeLock.release()
        return True

    def __setThreadCountNolock(self, newNumThreads):
        
        """Set the current pool size, spawning or terminating threads
        if necessary.  Internal use only; assumes the resizing lock is
        held."""
        while newNumThreads > len(self.__threads):
            threadClazz = globals()[self.threadName];
            """len(self.__threads) mean threadID"""
            newThread = threadClazz(self, len(self.__threads))
            self.__threads.append(newThread)
            newThread.start()
        # If we need to shrink the pool, do so
        while newNumThreads < len(self.__threads):
            self.__threads[0].goAway()
            del self.__threads[0]
    
    """
    note:
        deleleThread canot put in thread'goAway method
        if so when joinAll() require lock and call goAway  
        but goAway also require lock
    """
    def deleleThread(self, threadItem):
        self.__resizeLock.acquire()
        try:
            for index in range(0,len(self.__threads)):
                if threadItem == self.__threads[index]:
                    del self.__threads[index]
                    break;
            if len(self.__threads) == 0:
                self.available = False
        finally:           
            self.__resizeLock.release()
        
      

    def getThreadCount(self):

        """Return the number of threads in the pool."""
        
        self.__resizeLock.acquire()
        try:
            return len(self.__threads)
        finally:
            self.__resizeLock.release()

    def queueTask(self, task, args=None, taskCallback=None):

        """Insert a task into the queue.  task must be callable;
        args and taskCallback can be None."""
        
        if self.__isJoining == True:
            return False
        if not callable(task):
            return False
        
        self.__taskLock.acquire()
        try:
            self.__tasks.append((task, args, taskCallback))
            return True
        finally:
            self.__taskLock.release()

    def getNextTask(self):

        """ Retrieve the next task from the task queue.  For use
        only by ThreadPoolThread objects contained in the pool."""
        
        self.__taskLock.acquire()
        try:
            if self.__tasks == []:
                return (None, None, None)
            else:
                return self.__tasks.pop(0)
        finally:
            self.__taskLock.release()
    
    def getQueueSize(self):

        """ Retrieve the task queue size."""
        
        self.__taskLock.acquire()
        try:
            if self.__tasks == []:
                return 0
            else:
                return len(self.__tasks)
        finally:
            self.__taskLock.release()
            
    def joinAll(self, waitForTasks = True, waitForThreads = True):

        """ Clear the task queue and terminate all pooled threads,
        optionally allowing the tasks and threads to finish."""
        
        # Mark the pool as joining to prevent any more task queueing
        self.__isJoining = True

        # Wait for tasks to finish
        """
        think about all request timeout
        recv buff has all recv task
        so recvPool can set waitForTasks param False
        """
        if waitForTasks:
            while self.__tasks != []:
                print ("self.__tasks = %d" % len(self.__tasks))
                sleep(.1)

        # Tell all the threads to quit
        self.__resizeLock.acquire()
        try:
            self.__setThreadCountNolock(0)
            self.__isJoining = True

            # Wait until all threads have exited
            if waitForThreads:
                for t in self.__threads:
                    t.join()
                    del t
            # Reset the pool for potential reuse
            self.__isJoining = False
            
        finally:
            self.__resizeLock.release()


        
class FlexThreadPoolSendThread(threading.Thread):
    """ Pooled thread class. """
    threadSleepTime = 0.1;
    totalWaitTime = 0;

    def __init__(self, pool, threadID):
        """
        1. Initialize the thread and remember the pool. 
        2. sockets must be queue 
            when get task to send queue.get() sokcet 
            when server resposne  recvtask put socket into queue
            if queue is null ,we will wait 
            in this way we can implments block and high concurrency effect
        """
        threading.Thread.__init__(self)
        self.pool = pool
        self.sockets = Queue.Queue()
        self.isDying = False
        self.threadID = "send thread " + str(threadID) + " ";
        self.lockTime = pool.configDO.lockTime;
        self.maxSocketSize = pool.configDO.concurrentConnections;
        self.stepSocketSize = pool.configDO.stepSocketSize;
        self.curSocketSize = 0;
    
    def __del__(self):
        pass;
    
    """
    init conneciton 
        every time inited stepSocketSize connections and 
        self.curSocketSize < self.maxSocketSize
    """
    def initConnection(self):
        tryTimes = 0;
        successTimes = 0;
        while self.curSocketSize < self.maxSocketSize and \
                successTimes < self.stepSocketSize and self.isDying == False:
            if self.getConnection() == True:
                successTimes = successTimes + 1;
                
            tryTimes = tryTimes + 1;
            if tryTimes >= self.stepSocketSize * 3 :
                return ;
        print ("%s thread pool init %d Connection  sucessfully ! \n%s current connections is %d/%d"  \
                % (self.threadID, successTimes, self.threadID, self.curSocketSize, self.maxSocketSize))
       
        
    
  
    
    
    """
    getConenction :
        contains reconnect function
    """
    def getConnection(self):
        tryTimes = 3;
        sleepTime = 5;
        tryTime = 0;
        isConnected = False;
        while tryTimes > tryTime:
            """
            note:
                think about whether add if  len(self.pool.socketFactory.clients) > 0
                we will connection many times without if
            """
            self.connector = self.pool.socketFactory.connect(self.pool.socketFactory);
            """sleep wait to build connection """
            sleep(1)
            tryTime = tryTime + 1;
            if self.pool.socketFactory.socketSize() == 0:
                print ("%s find socketsList is 0 , begin to sleep %d s . tryTime is %d" % (self.threadID, sleepTime, tryTime))
                sleep(sleepTime);
            else:
                socket = self.pool.socketFactory.popSocket(0)
                if not socket is None:
                    if socket.connected == 1:
                        """ set curSocketSize and item into queue """
                        self.putSocket(socket);
                        isConnected = True
                        return isConnected
                    else:
                        socket.closeConnection()
                else:
                    print("%s %dth time get connection fails !" % (self.threadID, tryTime))
        return isConnected;
  
    """
    why dont we do self.curSocketSize = self.curSocketSize - 1 ?
    as curSocketSize is used to judge whether new connection can be created
    and although socket is get but recvTask will return the socket 
    
    but why should we do self.curSocketSize = self.curSocketSize - 1 ? 
    when connected = 0  as the socket is not effective 
    this means throw one away and create a new one socket
    """
    def getSocket(self):
        socket = self.sockets.get(True, self.lockTime);
#         self.curSocketSize = self.curSocketSize - 1;
        return socket;
    
    def putSocket(self,socket):
        self.sockets.put(socket, True, self.lockTime);
        self.curSocketSize = self.curSocketSize + 1;
    
    """
    remove disconnected connection 
    remove socketFacotry.clients , donot change self.curSocketSize
    """
    def removeDisconnected(self):
        if self.pool.socketFactory.clientsLock.acquire():
            try:
                connectedIndex = 0 
                extraSockets = len(self.pool.socketFactory.clients)
            
                """
                find which socket is connected
                """
                for connectedIndex in range(0, extraSockets):
                    socket = self.pool.socketFactory.clients[connectedIndex];
                    if socket.connected == 1:
                        break
            
                connectedIndex = connectedIndex - 1 
                while connectedIndex >= 0 :
                    self.pool.socketFactory.clients.pop(connectedIndex)
                    connectedIndex = connectedIndex - 1
            finally:
                self.pool.socketFactory.clientsLock.release();
        
            
   
    def reconnectSocket(self, socket):
        
        self.removeDisconnected();
        
        if self.getConnection() is False:
            pass;
        else:
            """
            if connected , maybe all sockets in queue is disconnected
            note :
                self.sockets.qsize() may be not equal to self.maxSocketSize  
                and self.curSocketSize may equal or less too !
            """
            socket = self.sockets.get(True, self.lockTime);
            while not socket is None and socket.connected == 0 and self.sockets.qsize() > 0:
                """ 
                out queue about why self.curSocketSize - 1  
                about curSocketSize see comment getSocket() method
                """
                self.curSocketSize = self.curSocketSize - 1;
                socket = self.sockets.get(True, self.lockTime);
                        
                """
                comment the getConnection.
                when in this while 
                    we can get one socket at least from sockets  
                    as if self.getConnection() is False thread will exit
                and if get socket and sockets is not enough 
                    we will connect the server when next call
                """
#                 self.getConnection();
        if socket.connected == 1:
            """ add  curSocketSize  as it is connected"""
            print("%s reconnect successfully !" % (self.threadID))
        else :
            print("%s reconnect fails !" % (self.threadID))
            
        return socket
        
      
        
    
    def run(self):
        
        """init part connection cost less time than init all connection"""
        self.initConnection();
        
        """if not connected kill self and remove from threadPool  """               
        if self.curSocketSize == 0:
            print("%s init socket is null ,thread kill self" % (self.threadID))
            self.goAway();
            self.pool.deleleThread(self);
        
        print("%s inited %d connection sucessfully !" % (self.threadID, self.curSocketSize))
        
        while self.isDying == False:
            try:
                """
                we get task  firstly
                if has no task ,we donot try to get socket 
                If there's nothing to do, just sleep a bit
                """
                cmd, args, callback = self.pool.getNextTask()
                if cmd is None:
                    sleep(self.threadSleepTime);
                    continue;
                
                """
                if socket is not enough and __currSocketSize < maxSocketSize
                we will do initConnection again
                """
                if self.sockets.qsize() == 0 and self.curSocketSize < self.maxSocketSize :
                    self.initConnection();
                
                """ if socket is not active try reconnect"""
                socket = self.getSocket();
                
                if socket.connected == 0:
                    """cannot connect"""
                    self.curSocketSize = self.curSocketSize - 1;
                    socket = self.reconnectSocket(socket);
                
                """recheck socket connect state"""
                if socket.connected == 0:
                    self.goAway();
                    self.pool.deleleThread(self);
                    break;
    
                """do task"""
                if callback is None:
                    cmd(socket, self.sockets)
                else:
                    callback(cmd(socket, self.sockets));
            except Exception, error:
                print("%s doTask error ! %s" % (self.threadID, error))
        print("%s exit" % (self.threadID))
        thread.exit();
    
    def goAway(self):
        """ 
        1.Exit the run loop next time through.
        2.release connection
        we must exit firstly, 
        if we release conection Firstly , 
        while self.isDying == False is doing 
        
        """
        self.isDying = True;
        print("%s begin to close %d connections" %(self.threadID, self.sockets.qsize()))
        while self.sockets.qsize() > 0:
            socket = self.sockets.get(True, self.lockTime);
            socket.closeConnection();
        
        
      

class FlexThreadPoolRecvThread(threading.Thread):

    """ Pooled thread class. """
    threadSleepTime = 0.1

    def __init__(self, pool, threadID):
        """ Initialize the thread and remember the pool. """
        threading.Thread.__init__(self)
        self.pool = pool
        self.isDying = False
        self.threadID = "recv thread " + str(threadID) + " ";
   
    def run(self):
        """ Until told to quit, retrieve the next task and execute
        it, calling the callback if any.  """
        print("%s init  successfully !" % self.threadID)
        """
        recvThread cannot be stopped by recvTask exception 
        if so all doCommand will timeout
        and sockets will be released  after all doCommand timeout
        it will be a long time
        """
        while self.isDying == False:
            try:
                cmd, args, callback = self.pool.getNextTask()
                """ If there's nothing to do, just sleep a bit """
                if cmd is None:
                    sleep(self.threadSleepTime)
                elif callback is None:
                    cmd();
                else:
                    callback(cmd());
            except Exception, error:
                print("%s do task error !! %s "  %(self.threadID, error))
            
                
        print("%s exit" % (self.threadID))
        thread.exit();
    
    def goAway(self):
        """ Exit the run loop next time through."""
        self.isDying = True



"""
twist connection
"""
class ClientSocket(Protocol):

    def __init__(self,socketFactory):
        self.socketFactory = socketFactory;
           
    def sendMessage(self, msg):
        self.transport.write(msg);

    """
    generate a connection call the method one time.
    one connection mean one ClientSocket's transport
    
    note all the twist event loop is the same threadID
        although twisted use one thread to process the event loop
        but threadpool may access clients
        so we need mutlti thread control
    """
    def connectionMade(self):
        self.socketFactory.appendSocket(self)
        
    
    def dataReceived(self, data):
        DataSourceManager.recvBuff.put(data);
        DataSourceManager.recvThreadPool.queueTask(DataSourceManager.recvTask);
       

    """
     close connection
    """
    def closeConnection(self):
        self.transport.loseConnection();
        """
        loseConnection will wait for data finished 
        abortConnection is not
        """
        #self.transport.abortConnection()
       
    def connectionLost(self, reason):
        self.connected = 0;
        print("Protocol connection lost")

   

   

class SocketFactory(ClientFactory):
    
    def __init__(self,configDO):
        """
        clients should do syncControl 
        twisted single thread model to put item
        thread pool use clients to pop item
        
        note:
            we can try connect many times for one connection
            and maybe more than one connection will be produced
        """
        self.clients = []
        self.clientsLock = threading.Condition(threading.Lock())
        self.configDO = configDO;
        
    def connect(self,socketFactory):
        connector = reactor.connectTCP(self.configDO.serverIP, self.configDO.serverPort,socketFactory);
        return  connector;


    def buildProtocol(self,addr):
        socket = ClientSocket(self);
        return socket

    def clientConnectionLost(self, connector, reason):
        print('SocketFactory Lost connection.  Reason:', reason)

    def clientConnectionFailed(self, connector, reason):
        print('SocketFactory failed. Reason:', reason)
        
    def appendSocket(self, socket):
        if self.clientsLock.acquire():
            try:
                self.clients.append(socket);
            finally:
                self.clientsLock.release();
                
    def socketSize(self):
        socketSize = 0;
        if self.clientsLock.acquire():
            try:
                socketSize = len(self.clients)
            finally:
                self.clientsLock.release();
        return socketSize;
    
    def popSocket(self, index = 0):
        socket = None
        if self.clientsLock.acquire():
            try:
                if len(self.clients) > 0:
                    socket = self.clients.pop(index)
            finally:
                self.clientsLock.release()
        return socket
                
    
    def releaseSockets(self):
        if self.clientsLock.acquire():
            try:
                for socket in self.clients:
                    socket.closeConnection();
                """reset """
                self.clients = []
            finally:
                self.clientsLock.release()






"""
@classmethod is similar to java static it can access class variable
    but not instance variable
@staticmethod cannot access class variable and instance variable
"""

class MessageUtil(object):
    def __init__(self):
        pass
    
    """
    cast client's requestInfoDO to requestInfoWrapperDO
    it is used to serial for net transfer and  deserial for server
    """
    @classmethod
    def getRequestWrapper(clazz,requestInfoDO):
        requestWrapper = requestInfoWrapper();
        
        if requestInfoDO is None:
            return requestWrapper;
        """
        dir(object) get class attr
        if callable  means just method 
        """
        methodList = [method for method in dir(requestInfoDO) if  callable(getattr(requestInfoDO,method))];
        for method in methodList:
            """len(method) > 3 as we will do method.substring(3)"""
            if len(method) > 3 and method.find("get") == 0:
                """
                method[3:]  get method name such as getAppKey --> AppKey
                [0].lower() first char to lower AppKey --> appKey
                """
                attrName = method[3:][0].lower() + method[3:][1:];
                if hasattr(requestInfoDO,attrName) and not getattr(requestInfoDO,attrName) is None:
                    attrVal = getattr(requestInfoDO,attrName);
                    setattr(requestWrapper,attrName,str(attrVal));
       
        return requestWrapper;

    

    """
    cast message responseInfoWrapper from server  into type ResponseInfoDO
    """
    @classmethod
    def getResponseInfo(clazz,responseWrapper):
        
        if responseWrapper is None:
            return None

        responseInfo = ResponseInfoDO();
        """
        dir(object) get class attr , if callable  means just method
        
        note dir(responseInfo) not dir(responseWrapper)
            as responseWrapper is generated by protocbuff
            it has more method than we expected
            but when getattr we get from responseWrapper
        """
        methodList = [method for method in dir(responseInfo) if  callable(getattr(responseInfo,method))];
        for method in methodList:
            if len(method) > 3 and method.find("get") == 0:
                attrName = method[3:][0].lower() + method[3:][1:];
                if hasattr(responseWrapper,attrName) and not getattr(responseWrapper,attrName) is None:
                    attrVal = getattr(responseWrapper,attrName);
                    setattr(responseInfo,attrName,str(attrVal));
       
        return responseInfo;
        



    """
    set request header info
    """
    @classmethod
    def setRequestHeader(clazz,requestInfo,configDO):
        
        requestInfo.setLinkPin(configDO.linkPin);
        requestInfo.setVersion(configDO.version);
        requestInfo.setAppKey(configDO.appKey);
        """
        messageID must set str type used to lockmap key
            in case of same value but different type
            so lockmap donnot get not None value

        messageID generated by UUID
            consist of mac-address,timestamp,random
        """
        messageID = str(uuid.uuid1());
        requestInfo.setMessageID(messageID);

        return requestInfo;



"""
request 
"""
class RequestInfoDO(object):
    def __init__(self, clusterName, custInsName, custInsID, masterIP, masterPort, slaveIP, slavePort, diskSize, dbType):
        self.version = None;
        self.linkPin = None;
        self.appKey = None;
        self.messageID = None;
        self.cmd = None;
        self.clusterName = None;
        self.custInsID = None;
        self.custInsName = None;
        self.masterIP = None;
        self.masterPort = None;
        self.slaveIP = None;
        self.slavePort = None;
        self.dbType = None;
        self.diskSize = None;
        self.sqlList = None;
        self.setClusterName(clusterName)
        self.setCustInsName(custInsName)
        self.setCustInsID(custInsID);
        self.setMasterIP(masterIP);
        self.setMasterPort(masterPort);
        self.setSlaveIP(slaveIP);
        self.setSlavePort(slavePort);
        self.setDiskSize(diskSize);
        self.setDbType(dbType);
    
#     def constructor(self, clusterName, custInsName, custInsID, masterIP, masterPort, slaveIP, slavePort, diskSize, dbType):
#         request = RequestInfoDO();
#         request.setClusterName(clusterName)
#         request.setCustInsName(custInsName)
#         request.setCustInsID(custInsID);
#         request.setMasterIP(masterIP);
#         request.setMasterPort(masterPort);
#         request.setSlaveIP(slaveIP);
#         request.setSlavePort(slavePort);
#         request.setDiskSize(diskSize);
#         request.setDbType(dbType);
#         return request;

    def setVersion(self, version):
        self.version = version;

    def getVersion(self):
        return self.version;

    def setLinkPin(self,linkPin):
        self.linkPin = linkPin;

    def getLinkPin(self):
        return self.linkPin;

    def setAppKey(self,appKey):
        self.appKey = appKey;

    def getAppKey(self):
        return self.appKey;

    def setMessageID(self,messageID):
        self.messageID = messageID;

    def getMessageID(self):
        return self.messageID;

    def setCmd(self,cmd):
        self.cmd = cmd;

    def getCmd(self):
        return self.cmd;

    def setClusterName(self,clusterName):
        self.clusterName = clusterName;

    def getClusterName(self):
        return self.clusterName;

    def setCustInsID(self,custInsID):
        self.custInsID = custInsID;

    def getCustInsID(self):
        return self.custInsID;

    def setCustInsName(self,custInsName):
        self.custInsName = custInsName;

    def getCustInsName(self):
        return self.custInsName;

    def setMasterIP(self,masterIP):
        self.masterIP = masterIP;

    def getMasterIP(self):
        return self.masterIP;

    def setMasterPort(self,masterPort):
        self.masterPort = masterPort;

    def getMasterPort(self):
        return self.masterPort;

    def setSlaveIP(self,slaveIP):
        self.slaveIP = slaveIP;

    def getSlaveIP(self):
        return self.slaveIP;

    def setSlavePort(self,slavePort):
        self.slavePort = slavePort;

    def getSlavePort(self):
        return self.slavePort;

    def setDbType(self,dbType):
        self.dbType = dbType;

    def getDbType(self):
        return self.dbType;

    def setDiskSize(self,diskSize):
        self.diskSize = diskSize;

    def getDiskSize(self):
        return self.diskSize;

    def setSqlList(self,sqlList):
        self.sqlList = sqlList;

    def getSqlList(self):
        return self.sqlList;

    def toString(self):
        buff = "RequestInfoDO is as follows : \n" ;
        buff = buff + "version : " + str(self.version) + "\n";
        buff = buff + "linkPin : " + str(self.linkPin) + "\n";
        buff = buff + "appKey : " + str(self.appKey) + "\n";
        buff = buff + "messageID : " + str(self.messageID) + "\n";
        buff = buff + "cmd : " + str(self.cmd) + "\n";
        buff = buff + "clusterName : " + str(self.clusterName) + "\n";
        buff = buff + "custInsID : " + str(self.custInsID) + "\n";
        buff = buff + "custInsName : " + str(self.custInsName) + "\n";
        buff = buff + "masterIP : " + str(self.masterIP) + "\n";
        buff = buff + "masterPort : " + str(self.masterPort) + "\n";
        buff = buff + "slaveIP : " + str(self.slaveIP) + "\n";
        buff = buff + "slavePort : " + str(self.slavePort) + "\n";
        buff = buff + "dbType : " + str(self.dbType) + "\n";
        buff = buff + "diskSize : " + str(self.diskSize) + "\n";
        buff = buff + "sqlList : " + str(self.sqlList) + "\n";
        return buff;


"""
server response
"""
class ResponseInfoDO(object):
    def __init__(self):
        self.messageID = None;
        self.errorCode = None;
        

    def setMessageID(self,messageID):
        self.messageID = messageID;

    def getMessageID(self):
        return self.messageID;

    def setErrorCode(self,errorCode):
        self.errorCode = errorCode;

    def getErrorCode(self):
        return self.errorCode;

    def toString(self):
        buff = "["
        buff = buff + "messageID : " + str(self.messageID) + ","
        buff = buff + "errorCode : " + str(self.errorCode) + ","
        buff = buff + "]"
        return buff
    
    
"""
sdk config
"""
class SDKConfigDO(object):

    """socket"""
    serverIP = None;
    serverPort = None;
    maxSession = 100;
    minSession = 10;
    idleTime = 60;
    """lock timeout time """
    lockTime = 100;
    reqBuffSize = 32768;

    """message"""
    version = 1;
    linkPin = "0000";
    appKey = None;

    """datasourceManage"""
    sendThreads = 2;
    recvThreads = 2;
    
    """
    concurrentConnection is number every sendThread bind connection
    which means the cocurrent number of single thread
    note:every connection is synchronized and cannot reuse util 
    server response or timeout
    """
    concurrentConnections = 2;
    """
    if concurrentConnections = 10 and stepSocketSize = 2
    threadpool will init 5 times to finish  connection
    """
    stepSocketSize = 1;
    
    def __init__(self,serverIP,serverPort,appKey):
        self.serverIP = serverIP;
        self.serverPort = serverPort;
        self.appKey = appKey;
     

    def toString(self):
        buff = "sdk config is as follows : \n" ;
        buff = buff + "serverIP : " + self.serverIP + "\n";
        buff = buff + "serverPort : " + str(self.serverPort) + "\n";
        buff = buff + "maxSession : " + str(self.maxSession) + "\n";
        buff = buff + "minSession : " + str(self.minSession) + "\n";
        buff = buff + "idleTime : " + str(self.idleTime) + "\n";
        buff = buff + "lockTime : " + str(self.lockTime) + "\n";
        buff = buff + "reqBuffSize : " + str(self.reqBuffSize) + "\n";
        buff = buff + "appKey : " + self.appKey + "\n";
        buff = buff + "sendThreads : " + str(self.sendThreads) + "\n";
        buff = buff + "recvThreads : " + str(self.recvThreads) + "\n";
        return buff;
    
    
    
def main():
    sdkConfig = SDKConfigDO("10.118.136.126",8888,"RDS")
    print(sdkConfig.toString())
    client = DriverManager.getInstance(sdkConfig).getCloudDBAClient()

    requestInfo = RequestInfoDO("local","mytest",123979, "120.26.148.148","3306","120.26.148.148","3306",100, "mysql")
    requestInfo.setCmd("TIMEDTUNING")
    responseInfo = client.doCommand(requestInfo)
    if not responseInfo is None:
        print(responseInfo.toString())
    DriverManager.release()
    print (DataSourceManager.toString())

if __name__ == "__main__":
    main()
