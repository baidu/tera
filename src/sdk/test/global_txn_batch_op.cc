#include <iostream>
#include <memory>
#include <regex>
#include <vector>
#include <string>
#include <algorithm>
#include <utility>
#include <unordered_map>
#include <functional>
#include <signal.h>
#include <unistd.h>

#include <gflags/gflags.h>

#include "tera.h"
#include "version.h"

DECLARE_string(flagfile);

using std::vector;
using std::string;
using std::cout;
using std::endl;
using std::pair;
using std::shared_ptr;
using std::unique_ptr;
using std::unordered_map;
using std::function;

using TxnPtr = shared_ptr<tera::Transaction>;
using RowMutationPtr = shared_ptr<tera::RowMutation>;
using ClientPtr = shared_ptr<tera::Client>;
using TablePtr = shared_ptr<tera::Table>;

struct RowkeyCfQu {
  RowkeyCfQu() = default;
  RowkeyCfQu(string rowkey, string cf, string qu) : rowkey_(rowkey), cf_(cf), qu_(qu) {}

  string rowkey_, cf_, qu_;
};
// Used for parsing operator string
using OperatorStructure =
    vector<pair<string,  // vector of Table operations, pair<tablename, rowkeys(vector)>
                vector<RowkeyCfQu>>>;  // vector of rowkey-cf-qus in a table

static unordered_map<string, string>& GetHelpCommand() {
  static unordered_map<string, string> help_commands;
  return help_commands;
}

static void InitHelpCommand() {
  auto& help_commands = GetHelpCommand();
  help_commands["cas"] =
      "Compare and set old_vals to new_vals across different Tables, Rows, and "
      "Columns atomically, usage:  \n"
      "    cas <Table1-rowkey1.cf1.qu1:rowkey2.cf2.qu2#Table2-rowkey3.cf3.qu3> "
      "<old_val1:oldval2:oldval3> <new_val1:new_val2:new_val3>";
  help_commands["get"] =
      "Get values across different Tables, Rows, and Columns atomically, "
      "usage:                            \n"
      "    get <Table1-rowkey1.cf1.qu1:rowkey2.cf2.qu2#Table2-rowkey3.cf3.qu3>";
  help_commands["put"] =
      "Put values across different Tables, Rows, and Columns atomically, "
      "usage:                            \n"
      "    put <Table1-rowkey1.cf1.qu1:rowkey2.cf2.qu2#Table2-rowkey3.cf3.qu3> "
      "<val1:val2:val3>";
}

static void PrintHelp(const string& str = "") {
  auto& help_commands = GetHelpCommand();
  if (str == "" || help_commands.find(str) == help_commands.end()) {
    for (auto& help_info : help_commands) {
      cout << help_info.first << " " << help_info.second << endl;
    }
  } else {
    cout << str << ": " << help_commands[str] << endl;
  }
}

static vector<string> split(const string& str, const char delimiter) {
  vector<string> res;
  string::size_type pos = 0;
  while (pos < str.size()) {
    string::size_type new_pos = str.find(delimiter, pos);
    if (new_pos == string::npos) {
      res.emplace_back(str.begin() + pos, str.end());
      break;
    } else {
      res.emplace_back(str.begin() + pos, str.begin() + new_pos);
    }
    pos = new_pos + 1;
  }
  return res;
}

static int64_t ParseOperatorStructure(const string& str, OperatorStructure& opst, size_t& num) {
  opst.clear();
  num = 0;
  vector<string> table_operations = split(str, '#');
  for (auto& table_op : table_operations) {
    vector<string> table_rowkey = split(table_op, '-');
    if (table_rowkey.size() != 2) {
      return -1;
    }

    opst.emplace_back(table_rowkey[0], vector<RowkeyCfQu>());
    vector<string> row_operations = split(table_rowkey[1], ':');
    for (auto& row_op : row_operations) {
      vector<string> rowkey_cf_qu = split(row_op, '.');
      if (rowkey_cf_qu.size() < 2 || rowkey_cf_qu.size() > 3) {
        return -1;
      }

      if (rowkey_cf_qu.size() == 3) {
        opst.back().second.emplace_back(rowkey_cf_qu[0], rowkey_cf_qu[1], rowkey_cf_qu[2]);
      } else {
        opst.back().second.emplace_back(rowkey_cf_qu[0], rowkey_cf_qu[1], "");
      }
      ++num;
    }
  }
  return 0;
}

static int64_t OpenTables(ClientPtr client, const OperatorStructure& opst,
                          unordered_map<string, TablePtr>& tables) {
  tables.clear();
  tera::ErrorCode ec;
  for (auto& table : opst) {
    string tablename = table.first;
    if (tables.find(table.first) == tables.end()) {
      tables.emplace(table.first, TablePtr(client->OpenTable(table.first, &ec)));
      if (!tables[table.first]) {
        cout << "open table: " << table.first << " failed" << endl;
        cout << ec.ToString() << endl;
        return -1;
      }
    }
  }
  return 0;
}

static int64_t PutOp(ClientPtr client, const vector<string>& args) {
  if (args.size() != 4) {
    cout << "Arguments Error: " << args.size() << ", need 4" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  OperatorStructure opst;
  size_t op_num = 0;
  if (ParseOperatorStructure(args[2], opst, op_num) != 0) {
    cout << "Parse Arguments Error" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  vector<string> val = split(args[3], ':');
  if (op_num != val.size()) {
    cout << "op size is not equal to val size" << endl;
    return -1;
  }

  unordered_map<string, TablePtr> tables;
  if (OpenTables(client, opst, tables) != 0) {
    return -1;
  }

  TxnPtr g_txn(client->NewGlobalTransaction());
  if (!g_txn) {
    cout << "open txn failed" << endl;
    return -1;
  }

  string result;
  for (auto& table : opst) {
    const string& tablename = table.first;
    const auto& row_cf_qu_list = table.second;
    for (auto& row_cf_qu : row_cf_qu_list) {
      const string& rowkey = row_cf_qu.rowkey_;
      const string& cf = row_cf_qu.cf_;
      const string& qu = row_cf_qu.qu_;

      unique_ptr<tera::RowReader> reader(tables[tablename]->NewRowReader(rowkey));
      reader->AddColumn(cf, qu);
      g_txn->Get(reader.get());
      if (reader->GetError().GetType() != tera::ErrorCode::kOK &&
          reader->GetError().GetType() != tera::ErrorCode::kNotFound) {
        std::cout << reader->GetError().ToString() << std::endl;
        return -1;
      }

      if (reader->Done()) {
        result += ":";
      } else {
        result += reader->Value() + ":";
      }
    }
  }

  if (!result.empty()) result.pop_back();

  auto val_iter = val.begin();
  for (auto& table : opst) {
    const string& tablename = table.first;
    const auto& row_cf_qu_list = table.second;
    unordered_map<string, RowMutationPtr> row_mutations;

    for (auto& row_cf_qu : row_cf_qu_list) {
      const string& rowkey = row_cf_qu.rowkey_;
      const string& cf = row_cf_qu.cf_;
      const string& qu = row_cf_qu.qu_;

      if (row_mutations.find(rowkey) == row_mutations.end()) {
        RowMutationPtr row_mutation(tables[tablename]->NewRowMutation(rowkey));
        row_mutations[rowkey] = row_mutation;
      }
      row_mutations[rowkey]->Put(cf, qu, *(val_iter++));
    }

    for (auto mutation : row_mutations) {
      g_txn->ApplyMutation(mutation.second.get());
    }
  }

  g_txn->Commit();
  if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
    std::cout << "commit failed: " << g_txn->GetError().ToString() << std::endl;
    cout << result << endl;
    return -1;
  }
  std::cout << "commit success" << std::endl;

  return 0;
}

static int64_t GetOp(ClientPtr client, const vector<string>& args) {
  if (args.size() != 3) {
    cout << "Arguments Error: " << args.size() << ", need 3" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  OperatorStructure opst;
  size_t op_num = 0;
  if (ParseOperatorStructure(args[2], opst, op_num) != 0) {
    cout << "Parse Arguments Error" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  unordered_map<string, TablePtr> tables;
  if (OpenTables(client, opst, tables) != 0) {
    return -1;
  }

  TxnPtr g_txn(client->NewGlobalTransaction());
  if (!g_txn) {
    cout << "open txn failed" << endl;
    return -1;
  }

  string result;
  for (auto& table : opst) {
    const string& tablename = table.first;
    const auto& row_cf_qu_list = table.second;
    for (auto& row_cf_qu : row_cf_qu_list) {
      const string& rowkey = row_cf_qu.rowkey_;
      const string& cf = row_cf_qu.cf_;
      const string& qu = row_cf_qu.qu_;

      unique_ptr<tera::RowReader> reader(tables[tablename]->NewRowReader(rowkey));
      reader->AddColumn(cf, qu);
      g_txn->Get(reader.get());
      if (reader->GetError().GetType() != tera::ErrorCode::kOK &&
          reader->GetError().GetType() != tera::ErrorCode::kNotFound) {
        std::cout << reader->GetError().ToString() << std::endl;
        return -1;
      }

      if (reader->Done()) {
        result += ":";
      } else {
        result += reader->Value() + ":";
      }
    }
  }

  if (!result.empty()) result.pop_back();
  cout << result << endl;
  return 0;
}

static int64_t CasOp(ClientPtr client, const vector<string>& args) {
  if (args.size() != 5) {
    cout << "Arguments Error: " << args.size() << ", need 5" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  OperatorStructure opst;
  size_t op_num = 0;
  if (ParseOperatorStructure(args[2], opst, op_num) != 0) {
    cout << "Parse Arguments Error" << endl;
    PrintHelp(args[1]);
    return -1;
  }

  unordered_map<string, TablePtr> tables;
  if (OpenTables(client, opst, tables) != 0) {
    return -1;
  }

  TxnPtr g_txn(client->NewGlobalTransaction());
  if (!g_txn) {
    cout << "open txn failed" << endl;
    return -1;
  }

  string cur_val;
  const string& old_val = args[3];
  const string& new_val = args[4];
  for (auto& table : opst) {
    const string& tablename = table.first;
    const auto& row_cf_qu_list = table.second;
    for (auto& row_cf_qu : row_cf_qu_list) {
      const string& rowkey = row_cf_qu.rowkey_;
      const string& cf = row_cf_qu.cf_;
      const string& qu = row_cf_qu.qu_;

      unique_ptr<tera::RowReader> reader(tables[tablename]->NewRowReader(rowkey));
      reader->AddColumn(cf, qu);
      g_txn->Get(reader.get());
      if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
        std::cout << g_txn->GetError().ToString() << std::endl;
        return -1;
      }

      if (reader->Done()) {
        cur_val += ":";
      } else {
        cur_val += reader->Value() + ":";
      }
    }
  }

  if (!cur_val.empty()) cur_val.pop_back();

  if (old_val != cur_val) {
    cout << "cas failed: NotEqual" << endl;
    return -1;
  }

  vector<string> new_val_list = split(new_val, ':');
  if (op_num != new_val_list.size()) {
    cout << "op size is not equal to val size" << endl;
    return -1;
  }

  auto val_iter = new_val_list.begin();
  for (auto& table : opst) {
    const string& tablename = table.first;
    const auto& row_cf_qu_list = table.second;
    unordered_map<string, RowMutationPtr> row_mutations;

    for (auto& row_cf_qu : row_cf_qu_list) {
      const string& rowkey = row_cf_qu.rowkey_;
      const string& cf = row_cf_qu.cf_;
      const string& qu = row_cf_qu.qu_;

      if (row_mutations.find(rowkey) == row_mutations.end()) {
        RowMutationPtr row_mutation(tables[tablename]->NewRowMutation(rowkey));
        row_mutations[rowkey] = row_mutation;
      }

      row_mutations[rowkey]->Put(cf, qu, *(val_iter++));
    }

    for (auto mutation : row_mutations) {
      g_txn->ApplyMutation(mutation.second.get());
    }
  }

  g_txn->Commit();
  if (g_txn->GetError().GetType() != tera::ErrorCode::kOK) {
    std::cout << "cas failed: " << g_txn->GetError().ToString() << std::endl;
    return -1;
  } else {
    std::cout << "cas success" << endl;
  }

  return 0;
}

static void SignalHandler(int) { _exit(0); }

int main(int argc, char* argv[]) {
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);
  ::google::ParseCommandLineFlags(&argc, &argv, true);

  vector<string> args(argv, argv + argc);
  InitHelpCommand();

  if (args.size() < 2) {
    PrintHelp();
    return 0;
  } else if (args[1] == "help") {
    if (args.size() > 2) {
      PrintHelp(args[2]);
      return 0;
    } else {
      PrintHelp();
      return 0;
    }
  } else if (args[1] == "version") {
    PrintSystemVersion();
    return 0;
  }

  unordered_map<string, function<int64_t(ClientPtr client, const vector<string>& args)>>
      command_table;
  command_table["put"] = PutOp;
  command_table["get"] = GetOp;
  command_table["cas"] = CasOp;

  if (command_table.find(args[1]) == command_table.end()) {
    cout << "Wrong Command" << endl;
    PrintHelp();
    return -1;
  }

  tera::ErrorCode ec;
  ClientPtr client(tera::Client::NewClient(FLAGS_flagfile, args[1], &ec));
  if (!client) {
    cout << "Create Client Failed: " << ec.ToString() << endl;
    return -1;
  }

  return command_table[args[1]](client, args);
}
