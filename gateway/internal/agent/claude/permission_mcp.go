package claude

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
)

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// RunPermissionMCP implements the private stdio MCP server Claude invokes for
// permission prompts. It contains no decision policy: requests are forwarded
// to the daemon over the owner-only Unix socket created for this turn.
func RunPermissionMCP(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	socketPath := ""
	network := "unix"
	for index := 0; index < len(args); index++ {
		if args[index] == "--socket" && index+1 < len(args) {
			socketPath = args[index+1]
			index++
		} else if args[index] == "--network" && index+1 < len(args) {
			network = args[index+1]
			index++
		}
	}
	if strings.TrimSpace(socketPath) == "" {
		fmt.Fprintln(stderr, "indexqube Claude permission helper: --socket is required")
		return 2
	}
	connection, err := net.Dial(network, socketPath)
	if err != nil {
		fmt.Fprintf(stderr, "indexqube Claude permission helper: connect: %v\n", err)
		return 1
	}
	defer connection.Close()

	input := bufio.NewScanner(stdin)
	input.Buffer(make([]byte, 64<<10), maxPermissionBytes)
	output := json.NewEncoder(stdout)
	bridgeInput := bufio.NewScanner(connection)
	bridgeInput.Buffer(make([]byte, 64<<10), maxPermissionBytes)
	bridgeOutput := json.NewEncoder(connection)
	for input.Scan() {
		var request jsonRPCRequest
		if err := json.Unmarshal(input.Bytes(), &request); err != nil {
			_ = output.Encode(jsonRPCResponse{
				JSONRPC: "2.0", ID: json.RawMessage("null"),
				Error: &jsonRPCError{Code: -32700, Message: "invalid JSON-RPC message"},
			})
			continue
		}
		if len(request.ID) == 0 {
			continue
		}
		response := jsonRPCResponse{JSONRPC: "2.0", ID: request.ID}
		switch request.Method {
		case "initialize":
			protocolVersion := "2025-06-18"
			var params struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			if json.Unmarshal(request.Params, &params) == nil && params.ProtocolVersion != "" {
				protocolVersion = params.ProtocolVersion
			}
			response.Result = map[string]any{
				"protocolVersion": protocolVersion,
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]string{"name": "indexqube-claude-permissions", "version": "1"},
			}
		case "ping":
			response.Result = map[string]any{}
		case "tools/list":
			response.Result = map[string]any{"tools": []any{map[string]any{
				"name":        "approval_prompt",
				"description": "Request a durable IndexQube decision for a Claude tool operation.",
				"inputSchema": map[string]any{
					"type": "object", "additionalProperties": false,
					"properties": map[string]any{
						"tool_name": map[string]string{"type": "string"},
						"input":     map[string]any{"type": "object", "additionalProperties": true},
					},
					"required": []string{"tool_name", "input"},
				},
			}}}
		case "tools/call":
			var call struct {
				Name      string            `json:"name"`
				Arguments permissionRequest `json:"arguments"`
			}
			if err := json.Unmarshal(request.Params, &call); err != nil || call.Name != "approval_prompt" {
				response.Error = &jsonRPCError{Code: -32602, Message: "invalid approval_prompt call"}
				break
			}
			if err := bridgeOutput.Encode(call.Arguments); err != nil || !bridgeInput.Scan() {
				response.Error = &jsonRPCError{Code: -32603, Message: "IndexQube approval bridge is unavailable"}
				break
			}
			var decision permissionResponse
			if err := json.Unmarshal(bridgeInput.Bytes(), &decision); err != nil {
				response.Error = &jsonRPCError{Code: -32603, Message: "IndexQube approval bridge returned an invalid decision"}
				break
			}
			payload, err := json.Marshal(decision)
			if err != nil {
				response.Error = &jsonRPCError{Code: -32603, Message: "IndexQube could not encode the approval decision"}
				break
			}
			response.Result = map[string]any{"content": []any{map[string]string{"type": "text", "text": string(payload)}}}
		default:
			response.Error = &jsonRPCError{Code: -32601, Message: "method not found"}
		}
		if err := output.Encode(response); err != nil {
			return 1
		}
	}
	if err := input.Err(); err != nil {
		fmt.Fprintf(stderr, "indexqube Claude permission helper: read MCP input: %v\n", err)
		return 1
	}
	return 0
}
