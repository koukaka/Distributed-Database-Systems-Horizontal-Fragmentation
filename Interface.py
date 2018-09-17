#!/usr/bin/python2.7
# Implemented by SUHAS VENKATESH MURTHY
# ASU ID - 1212886218
# Interface for the assignement
#

import psycopg2
import traceback

DATABASE_NAME = 'dds_assgn1'


def isTableAvailable(openconnection):
    isAvailable = False;

    try:
        cur = openconnection.cursor();
        cur.execute("select exists(select relname from pg_class where relname = 'ratings')");
        isAvailable = cur.fetchone()[0];
        cur.close();
    except Exception:
        traceback.print_exc();

    return isAvailable;


def inserttable(tablename, openconnection, userid,itemid,rating):

    insert_stmt = "insert into " + tablename + " values ( " \
                  + str(userid) + ", " + str(itemid) + ", " + str(rating) + " )"
    cur = openconnection.cursor()
    cur.execute(insert_stmt)
    openconnection.commit()


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    try:
        cur = openconnection.cursor()

        if isTableAvailable(openconnection) == False :
            cur.execute("create table " + ratingstablename +
                        "(userid integer, sep1 char ,movieid integer, sep2 char,"
                        "rating float, sep3 char,timeFromStart BIGINT)");
            openconnection.commit()

        cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
        openconnection.commit()

        # Dropping the seperator columns and the last column
        drop_stmt = 'Alter table ' + ratingstablename + ' Drop column sep1, Drop column sep2, ' \
                                                        'Drop column sep3,' \
                                                        ' Drop column timeFromStart'
        cur.execute(drop_stmt)
        openconnection.commit()

    except Exception :
        traceback.print_exc()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()
        create_stmt = ""
        lower_bound = 0.0
        upper_bound = 0.0
        maxrating = 5.0
        range_width = maxrating / numberofpartitions
        partition_tab_names = ['range_part' + str(i) for i in range(0,numberofpartitions)]

        for i in range(0,numberofpartitions):
            upper_bound = lower_bound + range_width
            if i is 0:
                create_stmt = 'create table ' + partition_tab_names[i] + \
                              ' as select * from ' + ratingstablename + \
                              ' where rating >= ' + str(lower_bound) + \
                              ' and rating <= ' + str(upper_bound)
            else:
                create_stmt = 'create table ' + partition_tab_names[i] + \
                              ' as select * from ' + ratingstablename + \
                              ' where rating > ' + str(lower_bound) + \
                              ' and rating <= ' + str(upper_bound)

            cur.execute(create_stmt)
            openconnection.commit()
            lower_bound += range_width


    except Exception:
        traceback.print_exc()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    try:
        cur = openconnection.cursor()

        partition_tab_names = ['rrobin_part' + str(i) for i in range(0, numberofpartitions)]
        create_stmt = ""
        for i in range(1,numberofpartitions + 1):
            create_stmt = "create table " + str(partition_tab_names[i-1]) \
                          + ' as with temporary_tab as ( select userid, movieid, rating, ROW_NUMBER() ' \
                            'OVER() as index from ' + \
                          ratingstablename + ') select userid, movieid, rating from temporary_tab' +\
                         ' where  ( index - ' \
                          + str(i) + ' ) % ' + str(numberofpartitions) + ' = 0'

            cur.execute(create_stmt)
            openconnection.commit()

    except Exception:
        traceback.print_exc()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    try:
        sel_stmt_partition_count = 'select COUNT(*) from information_schema.tables where table_name LIKE' + "'rrobin_part%'"
        cur = openconnection.cursor()
        cur.execute(sel_stmt_partition_count)
        res = cur.fetchall()
        partition_count = float(res[0][0])
        insert_loc = 0

        row_count = getRowCount(0,openconnection)

        for i in range(1,int(partition_count)):
            next_table_row_count = getRowCount(i,openconnection)
            if row_count > next_table_row_count:
                insert_loc = i
                break

            row_count = next_table_row_count

        inserttable('ratings',openconnection,userid,itemid,rating)
        inserttable("rrobin_part" + str(insert_loc),openconnection,userid,itemid,rating)

    except Exception:
        traceback.print_exc()

def getRowCount(table_suffix,openconnection):
    sel_stmt_row_count = 'select COUNT(*) from rrobin_part' +str(table_suffix)
    cur = openconnection.cursor()
    cur.execute(sel_stmt_row_count)
    res = cur.fetchall()

    return int(res[0][0])



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    try:
        sel_stmt = 'select COUNT(*) from information_schema.tables where table_name LIKE' + "'range_part%'"
        cur = openconnection.cursor()
        cur.execute(sel_stmt)
        res = cur.fetchall()
        count = float(res[0][0])
        rating_width = 5.0/count
        lower_bound = 0.0
        insert_loc = 0

        for i in range(0,int(count)):
            if (rating in range(0,int(lower_bound + rating_width))  or
                            lower_bound < rating <= (rating_width + lower_bound)):
                insert_loc = i
                break

            lower_bound = lower_bound + rating_width

        inserttable('ratings',openconnection,userid,itemid,rating)
        inserttable('range_part' + str(insert_loc),openconnection,userid,itemid,rating)

    except Exception:
        traceback.print_exc()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()