from redis import Redis
from redis.commands.search.field import TagField, NumericField

def test_issue():
    r = Redis()
    r.flushall()
    
    # create the index
    r.execute_command("FT.CREATE", "cmd-idx", "ON", "HASH", "SCHEMA", 
                      "location", "GEO", 
                      "gwLocation", "GEO",
                      "name", "TEXT", "NOSTEM", "WEIGHT", "1", "SORTABLE",
                      "lastSeen", "NUMERIC", "SORTABLE", "UNF",
                      "updated", "NUMERIC", "SORTABLE", "UNF", 
                      "created", "NUMERIC", "SORTABLE", "UNF",
                      "secured", "TAG", "SORTABLE",
                      "deleted", "TAG",
                      "tags", "TAG", "SEPARATOR", ",", "SORTABLE",
                      "notes", "TEXT", "WEIGHT", "1",
                      "meta", "TEXT", "WEIGHT", "1", "NOSTEM",
                      "gwid", "TAG",
                      "status", "TEXT", "WEIGHT", "1",
                      "gateway", "TEXT", "WEIGHT", "1",
                      "device", "TEXT", "WEIGHT", "1",
                      "notifs", "TEXT", "WEIGHT", "1",
                      "tid", "TAG", "CASESENSITIVE", "SORTABLE", "UNF",
                      "locate", "TEXT", "WEIGHT", "1",
                      "factoryKey", "TAG",
                      "type", "TAG",
                      "spaceId", "TAG",
                      "assignments", "TAG",
                      "agentId", "TAG",
                      "syncedAt", "NUMERIC", "SORTABLE", "UNF",
                      "sendToSync", "NUMERIC", "SORTABLE", "UNF",
                      
                      
                      
        )
    

    # set the hash in loop
    for i in range(100000):
        r.hmset(
            "dev:edgev3-3", 
            { 
                "tags": "mock,mock_edgev3",
                "locate": "{\"assetName\":\"mock_gps8_edgev3-3\",\"ownerId\":\"edgev3\",\"updated\":1709654432}",
                "agentId": "edgev3",
                "meta": "{\"assetId\":\"mock_gps8_edgev3-3\"}",
                "status": "{}",
                "factoryKey": "8BDB4B7B22892BC6090DD3BDEB386891",
                "spaceId": "mock.default",
                "type": "mock",
                "tid": "edgev3-3",
                "config": "{\"masterKey\":\"8BDB4B7B22892BC6090DD3BDEB386891\",\"factoryKey\":\"8BDB4B7B22892BC6090DD3BDEB386891\",\"k\":12,\"seedTime\":1709654431,\"la",
                "updated": "1709654432386",
                "secured": "1709654432386",
                "mver": "21",
                "syncedAt": "0",
                "name": "mock_gps8_edgev3-3",                
                "deleted": "null",
                "location": "-122.1 37.4",
                "sendToSync": "0",
                "created": "1709654432382",
                "gwid": "null",
                "notifs": "null",
                "device": "null",
                "lastSeen": "null",
                "lastCmd": "null",
                "gwids": "null",
                "drift": "null",
                "gateway": "null",
                "gwLocation": "null",
                "gpsLocation": "null",
                "firstNotif": "null",
                "time2first": "null",
            })


if __name__ == "__main__":
    test_issue()