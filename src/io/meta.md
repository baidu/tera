* Meta

class Meta:
  
  void ToMetaTableKeyValue(std::string* key, std::string* value);

class TalbeMeta : public Meta

class TabletMeta : public Meta

struct UnpackContent {

  TablePtr* table = NULL;
  
  std::vector<TabletPtr>* tablets = NULL;
  
}

WrtieMeta(std::map<MetaPtr, bool>);

SuspendMetaWrite(std::map<MetaPtr, WriteClosure* done, WriteClosure* done)

UnpackMeta(UnpackContent content);
