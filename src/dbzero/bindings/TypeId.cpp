#include "TypeId.hpp"

namespace std

{

    using namespace db0::bindings;

    ostream &operator<<(std::ostream &os, TypeId type_id) 
    {
        switch (type_id) {
            case TypeId::NONE : {
                os << "None";
            }
            break;

            case TypeId::INTEGER : {
                os << "int";
            }
            break;

            case TypeId::FLOAT : {
                os << "float";
            }
            break;

            case TypeId::STRING : {
                os << "str";
            }
            break;

            case TypeId::LIST : {
                os << "list";
            }
            break;

            case TypeId::DICT : {
                os << "dict";
            }
            break;

            case TypeId::SET : {
                os << "set";
            }
            break;

            case TypeId::DATETIME : {
                os << "datetime";
            }
            break;

            case TypeId::DATE : {
                os << "date";
            }
            break;

            case TypeId::TUPLE : {
                os << "tuple";
            }
            break;

            case TypeId::MEMO_OBJECT : {
                os << "db0.Object";
            }
            break;

            case TypeId::DB0_BLOCK : {
                os << "db0.Block";
            }
            break;

            case TypeId::DB0_PANDAS_DATAFRAME : {
                os << "db0.PandasDataFrame";
            }
            break;

            case TypeId::DB0_LIST : {
                os << "db0.List";
            }
            break;

            case TypeId::DB0_TUPLE : {
                os << "DB0Tuple";
            }
            break;

            case TypeId::DB0_SET : {
                os << "DB0Set";
            }
            break;

            default:
            break;
        }
        return os;
    }

}