
#ifndef _rm_h_
#define _rm_h_

//standard C libraries
#include <string.h>
#include <malloc.h>

//standard C++ libraries
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <cassert>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

//typedef unsigned TablePosition;

/*struct TabAttribute
{
	string name;
	AttrType type;
	AttrLength length;
	TablePosition position;
};*/


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  RBFM_ScanIterator rbfm_ScanIterator;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close(){
	  return rbfm_ScanIterator.close();
  }
};

class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(); 	// Destructor

  IX_ScanIterator ix_ScanIterator;
  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  static IndexManager *indexManagerPt;

  IXFileHandle ixfileHandle;

  Attribute IXattribute;

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


  RC catalogDescriptionPreparation(string tableName, vector<Attribute> &attributes);    //obtain system catalog attributes

  RC createTabRecord(void*data, string tableName, int tableID, string fileName, int systemTable);

  RC updateCol(int tableid,vector<Attribute> attributes);

  RC createColRcd(void * data,int tableid, Attribute attr, int position, int nullflag);

  int getNewTabID();

  int getTabID(const string &tableName);

  RC getVarChar(void *data,const string &str);

  int vc2Str(void *data,string &str);

  int checkSystemTab(const string &tableName);

  RecordBasedFileManager* rbfm=RecordBasedFileManager::instance();

// Extra credit work (10 points)
public:

  map<string, int> _tableMap;
  map<int, vector<Attribute> > _columnsMap;

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator
       );


  RC deleteTupleWithIndex(FileHandle& filehandle, vector<Attribute>& descriptor, RID rid);
  RC insertTupleWithIndex(FileHandle& filehandle, vector<Attribute>& descriptor, const void* data, RID rid);
  RC updateTupleWithIndex(FileHandle& fileHandle, vector<Attribute>& descriptor, const void* upData, RID rid);

protected:
  RelationManager();
  ~RelationManager();



};

#endif
