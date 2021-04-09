#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "RPCClient.h"
#include "utils.h"

typedef struct _ClientCmd {
    ReqType  cmdType;
    char     key[KV_KEYLEN_LIMIT];
    uint64_t klen;
    int64_t  value;
    uint64_t vlen;
} ClientCmd;

static int checkNumber(char * str) {
    for (int i = 0; i < strlen(str); i++) {
        if (str[i] < '0' || str[i] > '9') {
            return -1;
        }
    }
    return 0;
} 

int parseInput(char * buf, __out ClientCmd * cmd) {
    int ret = -1;
    char * p = strtok(buf, " ");
    char * parsed[3]; // parsed[0]: cmdName, parsed[1]: key, parsed[2]: value
    // fetch key and value to the parsed
    for (int i = 0; i < 3; i++) {
        parsed[i] = p;
        p = strtok(NULL, " ");
    }

    // parse commands
    if (!strcmp(parsed[0], "get") || !strcmp(parsed[0], "GET")) {
        // check if parsed[1] exists
        if (parsed[1] == NULL) {
            printf("Usage: %s key\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // copy the key to the ClientCmd
        memcpy(cmd->key, parsed[1], klen);
        // set other arguments
        cmd->cmdType = GET;
        cmd->klen = klen;
        return 0; // return success here
    } else if (!strcmp(parsed[0], "put") || !strcmp(parsed[0], "PUT")) {
        // check if both key and value exist
        if (parsed[1] == NULL || parsed[2] == NULL) {
            printf("Usage: %s key value\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // check if the second argument is a number
        ret = checkNumber(parsed[2]);
        if (ret < 0) {
            printf("Error: value should be an integer number\n");
            return -1;
        }
        // set cmd
        cmd->cmdType = PUT;
        memcpy(cmd->key, parsed[1], klen);
        cmd->klen = klen;
        cmd->value = atoll(parsed[2]);
        cmd->vlen = sizeof(int64_t);
        return 0; // return success here
    } else if (!strcmp(parsed[0], "del") || !strcmp(parsed[0], "DEL") || !strcmp(parsed[0], "delete")) {
        // check if the key exists
        if (parsed[1] == NULL) {
            printf("Usage: %s key\n", parsed[0]);
            return -1;
        }
        // check if the length of the key exceeds the limit
        int klen = strlen(parsed[1]);
        if (klen > KV_KEYLEN_LIMIT) {
            printf("Error: key should be less than %d characters\n", KV_KEYLEN_LIMIT);
            return -1;
        }
        // set cmd
        cmd->cmdType = DEL;
        memcpy(cmd->key, parsed[1], klen);
        cmd->klen = klen;
        return 0; // return success here
    } else if (!strcmp(parsed[0], "quit") || !strcmp(parsed[0], "q")) {
        exit(0);
    }
    else {
        // no match cmd
        printf("Error: command not supported\n");
        return -1;
    }
    return -1;
}

static int clientShellExe(RPCClient * client, ClientCmd * cmd) {
    int ret = -1;
    char buf[17] = {0};
    switch (cmd->cmdType) {
    case GET:
        ret = RPCClientKVGet1S(client, cmd->key, cmd->klen, &(cmd->value), &(cmd->vlen));
        break;
    case PUT:
        // memcpy(buf, cmd->key, 16);
        // printf("put %s %d\n", buf, cmd->value);
        ret = RPCClientKVPut(client, cmd->key, cmd->klen, (void *)&(cmd->value), cmd->vlen);
        break;
    case DEL:
        ret = RPCClientKVDel(client, cmd->key, cmd->klen);
        break;
    default:
        ret = -1;
    }
    return ret;
}

static void clientShellPrintRes(ClientCmd * cmd) {
    switch (cmd->cmdType) {
    case GET:
        printf("Success: value = %ld\n", cmd->value);
        break;
    case PUT:
    case DEL:
        printf("Success\n");
    default:
        break;
    }
}

int main() {
    RPCClient client;
    int ret = -1;
    
    // initClient
    printf("Initializing RPCClient\n");
    ret = initRPCClient(&client, SIMPLE);
    if (ret < 0) {
        printf("intiRPCClient failed\n");
        return -1;
    }

    while (1) {
        char buf[256];
        // cmdline hint
        printf("mykv >> ");

        // get input
        gets(&buf);
        
        // parse command
        ClientCmd cmd;
        ret = parseInput(buf, &cmd);
        if (ret < 0) {
            continue;
        }

        // execute command
        ret = clientShellExe(&client, &cmd);
        if (ret < 0) {
            printf("%s failed\n", buf);
            continue;
        }

        // print result
        clientShellPrintRes(&cmd);
    }
}