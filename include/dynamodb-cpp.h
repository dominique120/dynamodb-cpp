/*
 * Copyright Dominique Verellen. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#ifndef ALDDB_H
#define ALDDB_H

#include <map>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/Outcome.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/QueryRequest.h>
#include <aws/dynamodb/model/QueryResult.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/PutItemResult.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>
#include <aws/dynamodb/model/UpdateItemResult.h>
#include <aws/dynamodb/model/DeleteItemResult.h>
#include <aws/dynamodb/model/DeleteItemRequest.h>
#include <aws/dynamodb/model/ScanRequest.h>
#include <aws/dynamodb/model/ScanResult.h>

namespace alddb {
	class DynamoDB {
	public:

		struct PrimaryKey {
		public:
			void set_keys(Aws::DynamoDB::Model::GetItemRequest& req) {
				for (const auto& key : pk) {
					req.AddKey(key.first, key.second);
				}
			}

			void set_keys(Aws::DynamoDB::Model::UpdateItemRequest& req) {
				for (const auto& key : pk) {
					req.AddKey(key.first, key.second);
				}
			}

			void set_keys(Aws::DynamoDB::Model::DeleteItemRequest& req) {
				for (const auto& key : pk) {
					req.AddKey(key.first, key.second);
				}
			}

			void add_key_string(Aws::String keyname, Aws::String keyvalue) {
				Aws::DynamoDB::Model::AttributeValue value;
				value.SetS(keyvalue);
				pk.push_back(std::pair<Aws::String, Aws::DynamoDB::Model::AttributeValue>(keyname, value));
			}

			template<typename Number>
			void add_key_numer(Aws::String keyname, Number keyvalue) {
				Aws::DynamoDB::Model::AttributeValue value;
				value.SetN(Number);
				pk.push_back(std::pair<Aws::String, Aws::DynamoDB::Model::AttributeValue>(keyname, value));
			}

		private:
			std::vector<std::pair<Aws::String, Aws::DynamoDB::Model::AttributeValue>> pk;
		};


		// Basic Operations
		static inline void get_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, PrimaryKey& primary_key, nlohmann::json& result_out);
		static inline bool update_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const nlohmann::json& request, const std::string& table, PrimaryKey& primary_key);
		static inline bool delete_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, PrimaryKey& primary_key);

		// Put item
		static inline bool put_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const nlohmann::json& request, const std::string& table);

		// Queries
		static inline void query_with_expression(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, const Aws::String& key_name, const Aws::String& expression, const nlohmann::json& expression_values, nlohmann::json& result_out);

		// Scan
		static inline void scan_table_items_dynamo(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, nlohmann::json& result_out);


		// Make default client
		// This will load configuration files from the saved profile
		// Same as the ones used by the AWS CLI
		static inline std::unique_ptr<Aws::DynamoDB::DynamoDBClient> make_default_client();

	private:
		static inline void parse_collection(const Aws::Vector<Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>>& dynamo_result, nlohmann::json& json_out);
		static inline void parse_object(const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>& dynamo_result, nlohmann::json& json_out);
		static inline nlohmann::json parse_type(Aws::DynamoDB::Model::AttributeValue attr);
		static inline void compose_type(Aws::DynamoDB::Model::AttributeValue& attr, const nlohmann::json& json);
		static inline void compose_object(Aws::DynamoDB::Model::AttributeValue& attr, const nlohmann::json& json);
		static inline Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> build_operation_values(const nlohmann::json& json);
		static inline Aws::String build_operation_expression(const nlohmann::json& json, const std::string& operation);
	};
}

// utilities
std::unique_ptr<Aws::DynamoDB::DynamoDBClient> alddb::DynamoDB::make_default_client() {
	Aws::Client::ClientConfiguration clientConfig;
	auto cli =
		std::make_unique<Aws::DynamoDB::DynamoDBClient>(clientConfig);
	return cli;
}

Aws::String alddb::DynamoDB::build_operation_expression(const nlohmann::json& json, const std::string& operation) {
	std::stringstream ss;
	ss << operation << " ";
	for (const auto& item : json.items()) {
		ss << item.key() << " = :" << item.key() << ", ";
	}
	Aws::String expr = ss.str().c_str();
	expr.pop_back();
	expr.pop_back();
	return expr;
}

Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> alddb::DynamoDB::build_operation_values(const nlohmann::json& json) {
	Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> object;
	for (const auto& item : json.items()) {
		std::pair<Aws::String, Aws::DynamoDB::Model::AttributeValue> pair;

		Aws::DynamoDB::Model::AttributeValue temp_attr;

		Aws::String p1 = ":";
		Aws::String p2 = item.key().c_str();

		pair.first = p1 + p2;
		compose_type(temp_attr, item.value());
		pair.second = temp_attr;

		object.insert(pair);
	}
	return object;
}


// type parsing
void alddb::DynamoDB::compose_object(Aws::DynamoDB::Model::AttributeValue& attr, const nlohmann::json& json) {
	Aws::Map<Aws::String, const std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> object;
	for (const auto& item : json.items()) {
		std::pair<Aws::String, std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> pair;

		Aws::DynamoDB::Model::AttributeValue temp_attr;

		pair.first = item.key().c_str();
		compose_type(temp_attr, item.value());
		pair.second = Aws::MakeShared<Aws::DynamoDB::Model::AttributeValue>("", temp_attr);

		object.insert(pair);
	}
	attr.SetM(object);
}

void alddb::DynamoDB::parse_object(const Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>& dynamo_result, nlohmann::json& json_out) {
	for (const auto& element : dynamo_result) {
		json_out[element.first.c_str()] = parse_type(element.second);
	}
}

nlohmann::json alddb::DynamoDB::parse_type(Aws::DynamoDB::Model::AttributeValue attr) {
	if (attr.GetType() == Aws::DynamoDB::Model::ValueType::STRING) {
		return attr.GetS();
	} else if (attr.GetType() == Aws::DynamoDB::Model::ValueType::NUMBER) {
		return attr.GetN();
	} else if (attr.GetType() == Aws::DynamoDB::Model::ValueType::BOOL) {
		return attr.GetBool();
	} else if (attr.GetType() == Aws::DynamoDB::Model::ValueType::ATTRIBUTE_MAP) {
		nlohmann::json json;
		auto type = attr.GetM();
		for (const auto& element : type) {
			json[element.first.c_str()] = parse_type(*element.second);
		}
		return json;
	} else if (attr.GetType() == Aws::DynamoDB::Model::ValueType::ATTRIBUTE_LIST) {
		nlohmann::json json = nlohmann::json::array();
		auto type = attr.GetL();
		for (const auto& element : type) {
			nlohmann::json obj = parse_type(*element);
			json.push_back(obj);
		}
		return json;
	} else if (attr.GetType() == Aws::DynamoDB::Model::ValueType::NULLVALUE) {
		return nullptr;
	} else {
		return nullptr;
	}
}

void alddb::DynamoDB::compose_type(Aws::DynamoDB::Model::AttributeValue& attr, const nlohmann::json& json) {
	for (const auto& item : json.items()) {
		if (item.value().is_number_integer()) {
			attr.SetN(std::to_string(item.value().get<long>()).c_str());
		} else if (item.value().is_number_float()) {
			attr.SetN(item.value().get<double>());
		} else if (item.value().is_string()) {
			attr.SetS(item.value().get<std::string>().c_str());
		} else if (item.value().is_boolean()) {
			attr.SetBool(item.value().get<bool>());
		} else if (item.value().is_array()) {
			Aws::Vector< std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> array;

			for (const auto& array_object : item.value().items()) {
				Aws::DynamoDB::Model::AttributeValue temp_attr;
				compose_object(temp_attr, array_object.value());
				array.push_back(Aws::MakeShared<Aws::DynamoDB::Model::AttributeValue>("", temp_attr));
			}
			attr.SetL(array);
		} else if (item.value().is_object()) {
			Aws::Map<Aws::String, const std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> object;
			for (const auto& nested_item : item.value().items()) {
				std::pair<Aws::String, std::shared_ptr<Aws::DynamoDB::Model::AttributeValue>> pair;

				Aws::DynamoDB::Model::AttributeValue temp_attr;

				pair.first = nested_item.key().c_str();
				compose_type(temp_attr, nested_item.value());
				pair.second = Aws::MakeShared<Aws::DynamoDB::Model::AttributeValue>("", temp_attr);

				object.insert(pair);
			}
			attr.SetM(object);
		} else if (item.value().is_null()) {
			attr.SetNull(true);
		}
	}
}

void alddb::DynamoDB::parse_collection(const Aws::Vector<Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>>& dynamo_result, nlohmann::json& json_out) {
	json_out = nlohmann::json::array();
	for (const auto& object : dynamo_result) {
		nlohmann::json obj;
		parse_object(object, obj);
		json_out.push_back(obj);
	}
}


// Operations with a composite key
void alddb::DynamoDB::get_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, PrimaryKey& primary_key, nlohmann::json& result_out) {
	Aws::DynamoDB::Model::GetItemRequest req;

	// Set up the request
	req.SetTableName(table_name); // table name

	// Setup the composite key for the GetItemRequest
	primary_key.set_keys(req);

	// Retrieve the item's fields and values
	const Aws::DynamoDB::Model::GetItemOutcome& result = client->GetItem(req);
	if (result.IsSuccess()) {
		parse_object(result.GetResult().GetItem(), result_out);
	} else {
		std::cout << "Failed to get item: " << result.GetError().GetMessage() << std::endl;
	}
}

bool alddb::DynamoDB::update_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const nlohmann::json& request, const std::string& table, PrimaryKey& primary_key) {
	// Define TableName argument
	Aws::DynamoDB::Model::UpdateItemRequest uir;
	uir.SetTableName(table.c_str());
		
	primary_key.set_keys(uir);

	// set expression for SET
	uir.SetUpdateExpression(build_operation_expression(request, "SET"));

	// Construct attribute value argument
	uir.SetExpressionAttributeValues(build_operation_values(request));

	// Update the item
	const Aws::DynamoDB::Model::UpdateItemOutcome& result = client->UpdateItem(uir);
	if (!result.IsSuccess()) {
		std::cout << result.GetError().GetMessage() << std::endl;
		return false;
	}

	return true;
}

bool alddb::DynamoDB::delete_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, PrimaryKey& primary_key) {
	Aws::DynamoDB::Model::DeleteItemRequest req;

	primary_key.set_keys(req);

	// Set table name
	req.SetTableName(table_name);

	const Aws::DynamoDB::Model::DeleteItemOutcome& result = client->DeleteItem(req);
	if (result.IsSuccess()) {
		return true;
	} else {
		std::cout << "Failed to delete item: " << result.GetError().GetMessage();
		return false;
	}
}


// Put Item
bool alddb::DynamoDB::put_item(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const nlohmann::json& request, const std::string& table) {
	Aws::DynamoDB::Model::PutItemRequest pir;
	pir.SetTableName(table.c_str());

	// Add body
	for (const auto& element : request.items()) {
		Aws::DynamoDB::Model::AttributeValue attribute_value;
		compose_type(attribute_value, element.value());
		pir.AddItem(element.key().c_str(), attribute_value);
	}

	const Aws::DynamoDB::Model::PutItemOutcome outcome = client->PutItem(pir);
	if (!outcome.IsSuccess()) {
		std::cout << outcome.GetError().GetMessage() << std::endl;
		return false;
	}
	return true;
}


// Query
void alddb::DynamoDB::query_with_expression(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, const Aws::String& key_name, const Aws::String& expression, const nlohmann::json& expression_values, nlohmann::json& result_out) {
	Aws::DynamoDB::Model::QueryRequest query_request;
	query_request.SetTableName(table_name);
	if (!key_name.empty()) {
		query_request.SetIndexName(key_name);
	}

	query_request.SetKeyConditionExpression(expression);
	query_request.SetExpressionAttributeValues(build_operation_values(expression_values));

	// run the query
	const Aws::DynamoDB::Model::QueryOutcome& result = client->Query(query_request);
	if (!result.IsSuccess()) {
		std::cout << result.GetError().GetMessage() << std::endl;
	}

	alddb::DynamoDB::parse_collection(result.GetResult().GetItems(), result_out);
}


// Scan
// Not really recommended since the Adjecency Lists pattern(for which this library is designed)
// stores all records in a single table this will will return a huge resultset.
void alddb::DynamoDB::scan_table_items_dynamo(std::unique_ptr<Aws::DynamoDB::DynamoDBClient> client, const Aws::String& table_name, nlohmann::json& result_out) {

	Aws::DynamoDB::Model::ScanRequest scan_request;
	scan_request.SetTableName(table_name);

	// run the scan
	const Aws::DynamoDB::Model::ScanOutcome& result = client->Scan(scan_request);
	if (!result.IsSuccess()) {
		std::cout << result.GetError().GetMessage() << std::endl;
	}

	DynamoDB::parse_collection(result.GetResult().GetItems(), result_out);
}

#endif