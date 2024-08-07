#include <Common/Exception.h>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}
/// TODO: Do refactoring: we can derive from POCO::Json::Object
/// and we should replace Poco::JSON::Object to Poco::JSON::Object::Ptr
struct JSONUtils
{
    template <typename Value>
    static Value getOrThrow(const Poco::JSON::Object & object, const String & key)
    {
        if (!object.has(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Key {} is not found in JSON object", key);
        return object.getValue<Value>(key);
    }

    static String to_string(const Poco::JSON::Object & object)
    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(object, oss);

        return oss.str();
    }

    static Poco::JSON::Object::Ptr from_string(const String & json_str)
    {
        Poco::JSON::Parser parser;
        return parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();
    }
};
}
