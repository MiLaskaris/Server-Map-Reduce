# Server-Map-Reduce

This is part of the Distributed Systems course and it is a joint effort of the following people:

Vasilis Botsis,Chrysa Chrysafi, Garyfallia Dimitriou and Miltiadis Laskaris. 

It is a multi-threaded server of an end to end application organized under the dispatcher/worker model that was meant to be connected with a number of clients (Android app). 
The users could perform a given number of operations using the Android app. More specific, one of its main operations was to trigger the server whose job is to recommend another user (a user registered in our service and the database) using a Map Reduce algorithm.

The sequence of events of the server starts at the Main, where we call the Server class and end up intitiating the threads used by our Android app in order to communicate. 
Once we have established the communication, we can handle through the DAO layer the queries that the Android app sends to the server and we can start the map reduce process in order to efficiently extract the desired output and 
send it back to the user.
