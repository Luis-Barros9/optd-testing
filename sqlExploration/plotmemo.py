#!/usr/bin/env python3

import psycopg2

'''
conn = psycopg2.connect(database="jop",
                        host="localhost",
                        user="jop")'''

conn = psycopg2.connect(database="optd",
                        host="127.0.0.1",
                        port=5432,
                        user="optd",
                        password="password")

seen={}
e=1

cursor = conn.cursor()
cursor.execute("SELECT gid,op,lchild,rchild FROM memo ORDER BY gid")
print("digraph memo {")
for gid, op, lchild, rchild in cursor.fetchall():
    if gid == 0:
        continue
    if gid not in seen:
        seen[gid]=gid
        print('"g%d" [shape=box,label=%s]'%(gid,gid))
    print('"e%d" [shape=oval,label=%s]'%(e,op))
    print('"g%d" -> "e%d"'%(gid,e))
    if lchild != 0: print('"e%d" -> "g%d"'%(e,lchild))
    if rchild != 0: print('"e%d" -> "g%d"'%(e,rchild))
    e += 1
print("}")
