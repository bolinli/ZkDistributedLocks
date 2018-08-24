package org.zkdistributelocks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZkDistributeloskImpl implements Lock,Watcher {

    private ZooKeeper zk = null;
    //根节点
    private String ROOT_LOCK = "/locks";
    //竞争的资源
    private String lockName;
    //前一个等待的锁
    private String LAST_WAIT_LOCK;
    //当前锁
    private String CURRENT_LOCK;
    //计数器
    private CountDownLatch countDownLatch;
    private int sessionTimeout = 30000;
    private List<Exception> exceptionsList = new ArrayList<>();

    /**
     * 配置分布式锁
     * */
    public ZkDistributeloskImpl(String config, String lockName){
        this.lockName = lockName;
        try{
            zk = new ZooKeeper(config,sessionTimeout,this);
            Stat stat = zk.exists(ROOT_LOCK,false);
            if(stat == null){
                zk.create(ROOT_LOCK,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if(exceptionsList.size() > 0){
            throw new LockException("锁名有误");
        }
        try{
            if(this.tryLock()){
                return;
            }else{
                waitForLock(LAST_WAIT_LOCK,sessionTimeout);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    /**
     * 等待锁
     * */
    private boolean waitForLock(String last_wait_lock, long sessionTimeout) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ROOT_LOCK + "/" + last_wait_lock, true);

        if(stat!=null){
            System.out.println(Thread.currentThread().getName() + "等待锁" + ROOT_LOCK + "/" + last_wait_lock);
            this.countDownLatch = new CountDownLatch(1);
            //计数等待 若等到前一个节点消失 则Precess中进行countDown 停止等待 获取锁
            this.countDownLatch.await(sessionTimeout,TimeUnit.MILLISECONDS);
            this.countDownLatch = null;
        }
        return true;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }

    @Override
    public boolean tryLock() {
        try{
            String splitStr = "_lock_";
            if(lockName.contains(splitStr)){
                throw new LockException("锁名有误");
            }
            //创建临时有序节点
            CURRENT_LOCK = zk.create(ROOT_LOCK + "/" + lockName + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //取所有子节点
            List<String> subNodes = zk.getChildren(ROOT_LOCK,false);
            //取出所有lockName的锁
            List<String> lockObjects = new ArrayList<>();
            for(String node:subNodes){
                String _node = node.split(splitStr)[0];
                if(_node.equals(lockName)){
                    lockObjects.add(node);
                }
            }
            Collections.sort(lockObjects);
            if (CURRENT_LOCK.equals(ROOT_LOCK + "/" + lockObjects.get(0))) {
                return true;
            }
            //如果不是最小节点 则找到自己的前一个节点
            String prevNode = CURRENT_LOCK.substring(CURRENT_LOCK.lastIndexOf("/")+1);
            LAST_WAIT_LOCK = lockObjects.get(Collections.binarySearch(lockObjects,prevNode)-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        try{
            zk.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    /**
     * 节点监视器
     * */
    @Override
    public void process(WatchedEvent event){
        if(this.countDownLatch != null){
            //减计数 如果计数为0则释放线程
            this.countDownLatch.countDown();
        }
    }

    public class LockException extends RuntimeException{
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }
}
