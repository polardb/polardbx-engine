#include <google/protobuf/compiler/js/well_known_types_embed.h>

struct FileToc well_known_types_js[] = {
    {"any.js",
     "/* This code will be inserted into generated code for\n"
     " * google/protobuf/any.proto. */\n"
     "\n"
     "/**\n"
     " * Returns the type name contained in this instance, if any.\n"
     " * @return {string|undefined}\n"
     " */\n"
     "proto.google.protobuf.Any.prototype.getTypeName = function() {\n"
     "  return this.getTypeUrl().split('/').pop();\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Packs the given message instance into this Any.\n"
     " * @param {!Uint8Array} serialized The serialized data to pack.\n"
     " * @param {string} name The type name of this message object.\n"
     " * @param {string=} opt_typeUrlPrefix the type URL prefix.\n"
     " */\n"
     "proto.google.protobuf.Any.prototype.pack = function(serialized, name,\n"
     "                                                    opt_typeUrlPrefix) "
     "{\n"
     "  if (!opt_typeUrlPrefix) {\n"
     "    opt_typeUrlPrefix = 'type.googleapis.com/';\n"
     "  }\n"
     "\n"
     "  if (opt_typeUrlPrefix.substr(-1) != '/') {\n"
     "    this.setTypeUrl(opt_typeUrlPrefix + '/' + name);\n"
     "  } else {\n"
     "    this.setTypeUrl(opt_typeUrlPrefix + name);\n"
     "  }\n"
     "\n"
     "  this.setValue(serialized);\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * @template T\n"
     " * Unpacks this Any into the given message object.\n"
     " * @param {function(Uint8Array):T} deserialize Function that will "
     "deserialize\n"
     " *     the binary data properly.\n"
     " * @param {string} name The expected type name of this message object.\n"
     " * @return {?T} If the name matched the expected name, returns the "
     "deserialized\n"
     " *     object, otherwise returns null.\n"
     " */\n"
     "proto.google.protobuf.Any.prototype.unpack = function(deserialize, name) "
     "{\n"
     "  if (this.getTypeName() == name) {\n"
     "    return deserialize(this.getValue_asU8());\n"
     "  } else {\n"
     "    return null;\n"
     "  }\n"
     "};\n"},
    {"timestamp.js",
     "/* This code will be inserted into generated code for\n"
     " * google/protobuf/timestamp.proto. */\n"
     "\n"
     "/**\n"
     " * Returns a JavaScript 'Date' object corresponding to this Timestamp.\n"
     " * @return {!Date}\n"
     " */\n"
     "proto.google.protobuf.Timestamp.prototype.toDate = function() {\n"
     "  var seconds = this.getSeconds();\n"
     "  var nanos = this.getNanos();\n"
     "\n"
     "  return new Date((seconds * 1000) + (nanos / 1000000));\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Sets the value of this Timestamp object to be the given Date.\n"
     " * @param {!Date} value The value to set.\n"
     " */\n"
     "proto.google.protobuf.Timestamp.prototype.fromDate = function(value) {\n"
     "  this.setSeconds(Math.floor(value.getTime() / 1000));\n"
     "  this.setNanos(value.getMilliseconds() * 1000000);\n"
     "};\n"},
    {"struct.js",
     "/* This code will be inserted into generated code for\n"
     " * google/protobuf/struct.proto. */\n"
     "\n"
     "/**\n"
     " * Typedef representing plain JavaScript values that can go into a\n"
     " *     Struct.\n"
     " * @typedef {null|number|string|boolean|Array|Object}\n"
     " */\n"
     "proto.google.protobuf.JavaScriptValue;\n"
     "\n"
     "\n"
     "/**\n"
     " * Converts this Value object to a plain JavaScript value.\n"
     " * @return {?proto.google.protobuf.JavaScriptValue} a plain JavaScript\n"
     " *     value representing this Struct.\n"
     " */\n"
     "proto.google.protobuf.Value.prototype.toJavaScript = function() {\n"
     "  var kindCase = proto.google.protobuf.Value.KindCase;\n"
     "  switch (this.getKindCase()) {\n"
     "    case kindCase.NULL_VALUE:\n"
     "      return null;\n"
     "    case kindCase.NUMBER_VALUE:\n"
     "      return this.getNumberValue();\n"
     "    case kindCase.STRING_VALUE:\n"
     "      return this.getStringValue();\n"
     "    case kindCase.BOOL_VALUE:\n"
     "      return this.getBoolValue();\n"
     "    case kindCase.STRUCT_VALUE:\n"
     "      return this.getStructValue().toJavaScript();\n"
     "    case kindCase.LIST_VALUE:\n"
     "      return this.getListValue().toJavaScript();\n"
     "    default:\n"
     "      throw new Error('Unexpected struct type');\n"
     "  }\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Converts this JavaScript value to a new Value proto.\n"
     " * @param {!proto.google.protobuf.JavaScriptValue} value The value to\n"
     " *     convert.\n"
     " * @return {!proto.google.protobuf.Value} The newly constructed value.\n"
     " */\n"
     "proto.google.protobuf.Value.fromJavaScript = function(value) {\n"
     "  var ret = new proto.google.protobuf.Value();\n"
     "  switch (goog.typeOf(value)) {\n"
     "    case 'string':\n"
     "      ret.setStringValue(/** @type {string} */ (value));\n"
     "      break;\n"
     "    case 'number':\n"
     "      ret.setNumberValue(/** @type {number} */ (value));\n"
     "      break;\n"
     "    case 'boolean':\n"
     "      ret.setBoolValue(/** @type {boolean} */ (value));\n"
     "      break;\n"
     "    case 'null':\n"
     "      ret.setNullValue(proto.google.protobuf.NullValue.NULL_VALUE);\n"
     "      break;\n"
     "    case 'array':\n"
     "      ret.setListValue(proto.google.protobuf.ListValue.fromJavaScript(\n"
     "          /** @type{!Array} */ (value)));\n"
     "      break;\n"
     "    case 'object':\n"
     "      ret.setStructValue(proto.google.protobuf.Struct.fromJavaScript(\n"
     "          /** @type{!Object} */ (value)));\n"
     "      break;\n"
     "    default:\n"
     "      throw new Error('Unexpected struct type.');\n"
     "  }\n"
     "\n"
     "  return ret;\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Converts this ListValue object to a plain JavaScript array.\n"
     " * @return {!Array} a plain JavaScript array representing this List.\n"
     " */\n"
     "proto.google.protobuf.ListValue.prototype.toJavaScript = function() {\n"
     "  var ret = [];\n"
     "  var values = this.getValuesList();\n"
     "\n"
     "  for (var i = 0; i < values.length; i++) {\n"
     "    ret[i] = values[i].toJavaScript();\n"
     "  }\n"
     "\n"
     "  return ret;\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Constructs a ListValue protobuf from this plain JavaScript array.\n"
     " * @param {!Array} array a plain JavaScript array\n"
     " * @return {proto.google.protobuf.ListValue} a new ListValue object\n"
     " */\n"
     "proto.google.protobuf.ListValue.fromJavaScript = function(array) {\n"
     "  var ret = new proto.google.protobuf.ListValue();\n"
     "\n"
     "  for (var i = 0; i < array.length; i++) {\n"
     "    "
     "ret.addValues(proto.google.protobuf.Value.fromJavaScript(array[i]));\n"
     "  }\n"
     "\n"
     "  return ret;\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Converts this Struct object to a plain JavaScript object.\n"
     " * @return {!Object<string, !proto.google.protobuf.JavaScriptValue>} a "
     "plain\n"
     " *     JavaScript object representing this Struct.\n"
     " */\n"
     "proto.google.protobuf.Struct.prototype.toJavaScript = function() {\n"
     "  var ret = {};\n"
     "\n"
     "  this.getFieldsMap().forEach(function(value, key) {\n"
     "    ret[key] = value.toJavaScript();\n"
     "  });\n"
     "\n"
     "  return ret;\n"
     "};\n"
     "\n"
     "\n"
     "/**\n"
     " * Constructs a Struct protobuf from this plain JavaScript object.\n"
     " * @param {!Object} obj a plain JavaScript object\n"
     " * @return {proto.google.protobuf.Struct} a new Struct object\n"
     " */\n"
     "proto.google.protobuf.Struct.fromJavaScript = function(obj) {\n"
     "  var ret = new proto.google.protobuf.Struct();\n"
     "  var map = ret.getFieldsMap();\n"
     "\n"
     "  for (var property in obj) {\n"
     "    var val = obj[property];\n"
     "    map.set(property, proto.google.protobuf.Value.fromJavaScript(val));\n"
     "  }\n"
     "\n"
     "  return ret;\n"
     "};\n"},
    {NULL, NULL}  // Terminate the list.
};
