
feedback_request_example = {
    "No Request body": {
        "summary": "Return a list of catalog",
        "value": "null"
    },
    "query": {
        "summary": "Query all active feedback for a specifc user",
        "value": {
            "action": "query",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
        }
    },
    "save": {
        "summary": "Add a new feedback for a specific user",
        "value": {
            "action": "save",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "catalog": "Bugs",
            "feedback": "Add a Feedback"
        }
    },
    "update": {
        "summary": "Update a specific feedback for a user",
        "value": {
            "action": "update",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "feedback_uuid": "25a4b1e7-3459-47b4-b091-b419a5c34db8",
            "catalog": "Bugs",
            "feedback": "Update a feedback"
        }
    },
    "delete": {
        "summary": "delete(set active to false) a specific feedback",
        "value": {
            "action": "delete",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "feedback_uuid": "25a4b1e7-3459-47b4-b091-b419a5c34db8",
        }
    },
}

weights_criterias_example = {
    "No Request body": {
        "summary": "Return the list of suitability weight variables",
        "value": "null"
    },
    "query": {
        "summary": "Query all suitability weight setting for a specific user",
        "value": {
            "action": "query",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
        }
    },
    "query2": {
        "summary": "Query a specific suitability weight setting for a user",
        "value": {
            "action": "query",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "settingname" : "TestingWeigh"
        }
    },
    "save": {
        "summary": "Add a new weight setting for a specific user",
        "value": {
            "action": "save",
            "settingname": "NewSetting",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "data": {
                "keys": ['key1','key2','key10','key11','key3','key12','key13','key4','key5','key6','key7','key8','key9'],
                "values": [0.2,0.5,0.5,0.1,0.1,0.1,0.1,0.1,0.7,0.4,0.6,0.7,0.3]
            }
        }
    },
    "update": {
        "summary": "Update a specific suitability weight setting for a user",
        "value": {
            "action": "update",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "settingname": "NewSetting",
            "data": {
                "keys": ['key1','key2','key10','key11','key3','key12','key4','key5','key6','key7','key8','key9'],
                "values": [0.5,0.1,0.6,0.1,0.1,0.1,0.1,0.1,0.7,0.4,0.6,0.7,0.3]
            }
        }
    },
    "delete": {
        "summary": "delete(set active to false) a specific suitability weight setting for a user",
        "value": {
            "action": "delete",
            "uuid": "ec365f74-5412-4365-9090-1dffef90671f",
            "settingname": "NewSetting",
        }
    },
}


