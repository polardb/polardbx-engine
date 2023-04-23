--connect (paxos1,127.0.0.1,root,,test,$MYPORT_1)
--connect (paxos2,127.0.0.1,root,,test,$MYPORT_2)
--connect (paxos3,127.0.0.1,root,,test,$MYPORT_3)

connection paxos1;
--let $PAXOS_PORT_1=`select @@port + 40;`
connection paxos2;
--let $PAXOS_PORT_2=`select @@port + 40;`
connection paxos3;
--let $PAXOS_PORT_3=`select @@port + 40;`
connection default;
