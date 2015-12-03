                    DISTRIBUTED COMPUTING (CS 380D)
                           PROJECT 3 : BAYOU

SLIP DAYS:
Used for this project 2
Total used for all projects 6

TEAM INFO:
  
Name                    UT Eid    CS Id    
1. KUNAL LAD            KL28697   klad@cs.utexas.edu
2. ASHWINI VENKATESH    AV28895   ashuven6@cs.utexas.edu

A) Commands to run:
 
# Assuming that you are in bayou directory obtained by unzipping the zip file.

cd src

# This script will build bayou project and run tester.py.
# tester.py expects tests and solution files in src/tests and src/solutions.

./executeBayou.sh

C) BAYOU DESIGN:

HEIRARCHY OF CLASSES : Indentation indicates the hierarchy.

Master
Client
    -SessionManager
Server
    -DataStore
    -WriteLog    

D) MAJOR DESIGN DECISIONS:

1. Creation:

- New server joins the system and sends a create request to some available 
  server. 

- New server waits for create response which consists of the write ID 
  of the CREATE write performed by server contacted in above step.

- The new server adopts this write ID as its server ID and sets its accept stamp
  to the accept stamp received from the create response.      

2. Retirement:

- Server starts by writing a RETIRE to its own write log.
 
- It then waits for an anti entropy request from some server.

- On receiving anti entropy request, it sends an anti entropy response
  followed by a RETIRE_REQ to this server.
  
- The receiving server makes itself as primary if it received retire from a 
  primary server. On becoming the primary it also commits all the tentative 
  writes it has in its write log.

3. Stabilize:
   
- On receiving a stabilize command, Master waits for 
  (2 * n * AntiEntropyTimePeriod) seconds.
   
  NOTE : We are being pessimistic here and using a weaker bound.
  For a network with diameter (i.e. max distance) n-1 we are sure that after
  n-1 Anti-Entropy rounds all nodes in the network would have stabilized. 
            
