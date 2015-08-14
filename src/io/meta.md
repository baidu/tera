* Meta

class Meta:
  
  void ToMetaTableKeyValue(std::string* key, std::string* value);

class TalbeMeta : public Meta

class TabletMeta : public Meta

* meta_io:

struct UnpackContent {
  TablePtr* table = NULL;
  std::vector<TabletPtr>* tablets = NULL;
}

struct ScanContent {
  std::vector<TalbeMeta*>* table_metas = NULL;
  std::vector<TalbetMeta*>* tablet_metas = NULL;
}

WrtieMeta(std::map<MetaPtr, bool>);

SuspendMetaWrite(std::map<MetaPtr, WriteClosure* done, WriteClosure* done)

UnpackMeta(UnpackContent content);

ReadTable(tablename, ScanTabletRequest* request, ScanTabletReponse* response);

ReadTablets(tablename, tablet_key_start, tablet_key_end, ScanTabletRequest* request, ScanTabletReponse* response);

ScanMeta(ScanContent content); // 用户需要析构conetent中内容
