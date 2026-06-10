// Package logging implements the distributed log querier: a cluster-wide
// parallel grep over each node's log file, queried via the client port.
package logging

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GrepResult represents the result from a single VM's grep query
type GrepResult struct {
	Hostname  string
	VMNumber  int
	Output    string
	Error     error
	LineCount int
}

// QueryDistributed runs grep on all alive VMs in the cluster concurrently
// Returns formatted results from all VMs
func QueryDistributed(grepArgs []string, aliveVMs map[string]int) (string, error) {
	results := make([]GrepResult, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Query each VM concurrently
	for hostname, vmNum := range aliveVMs {
		wg.Add(1)
		go func(host string, num int) {
			defer wg.Done()

			result := GrepResult{
				Hostname: host,
				VMNumber: num,
			}

			// Connect to VM's client port (8003)
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:8003", host), 3*time.Second)
			if err != nil {
				result.Error = fmt.Errorf("connection failed: %v", err)
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
				return
			}
			defer conn.Close()

			// Send dgrep_query command with args
			cmd := fmt.Sprintf("dgrep_query %s\n", strings.Join(grepArgs, " "))
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if _, err := conn.Write([]byte(cmd)); err != nil {
				result.Error = fmt.Errorf("send failed: %v", err)
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
				return
			}

			// Read response
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			scanner := bufio.NewScanner(conn)
			scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 1MB buffer

			var output strings.Builder
			lineCount := 0
			for scanner.Scan() {
				line := scanner.Text()
				if line == "END_DGREP" {
					break
				}
				if line != "" {
					output.WriteString(line)
					output.WriteString("\n")
					lineCount++
				}
			}

			if err := scanner.Err(); err != nil {
				result.Error = fmt.Errorf("read failed: %v", err)
			} else {
				result.Output = output.String()
				result.LineCount = lineCount
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(hostname, vmNum)
	}

	wg.Wait()

	// Sort results by VM number for consistent ordering
	sort.Slice(results, func(i, j int) bool {
		return results[i].VMNumber < results[j].VMNumber
	})

	// Format results
	var formatted strings.Builder
	totalLines := 0
	successCount := 0

	for _, result := range results {
		fmt.Fprintf(&formatted, "VM%02d (%s):\n", result.VMNumber, result.Hostname)
		formatted.WriteString("────────────────────────────────────────\n")

		if result.Error != nil {
			fmt.Fprintf(&formatted, "  ERROR: %v\n\n", result.Error)
		} else if result.Output == "" {
			formatted.WriteString("  No matches\n\n")
			successCount++
		} else {
			fmt.Fprintf(&formatted, "  %d lines matched:\n", result.LineCount)
			// Show first 5 lines as preview
			lines := strings.Split(strings.TrimSpace(result.Output), "\n")
			previewLines := 5
			if len(lines) < previewLines {
				previewLines = len(lines)
			}
			for i := 0; i < previewLines; i++ {
				fmt.Fprintf(&formatted, "  %s\n", lines[i])
			}
			if len(lines) > previewLines {
				fmt.Fprintf(&formatted, "  ... (%d more lines)\n", len(lines)-previewLines)
			}
			formatted.WriteString("\n")
			totalLines += result.LineCount
			successCount++
		}
	}

	formatted.WriteString("════════════════════════════════════════\n")
	fmt.Fprintf(&formatted, "Summary: %d/%d VMs responded successfully\n", successCount, len(results))
	fmt.Fprintf(&formatted, "Total matching lines: %d\n", totalLines)

	return formatted.String(), nil
}

// Query runs the grep command on the local machine's log file.
// It returns the string output of grep and any errors.
func Query(grepArgs []string) (string, error) {
	// Get hostname of this machine
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}

	// Get the machine number to find the correct log file
	machineNum, err := getMachineNumber(hostname)
	if err != nil {
		return "", err
	}

	// Find the most recent log file. Nodes write logs/node<N>_<timestamp>.log
	// (see the log setup in main); older deployments used vm<NN>_*.log.
	logPattern := fmt.Sprintf("logs/node%d_*.log", machineNum)
	matches, err := filepath.Glob(logPattern)
	if err != nil || len(matches) == 0 {
		logPattern = fmt.Sprintf("logs/vm%02d_*.log", machineNum)
		matches, _ = filepath.Glob(logPattern)
	}

	if len(matches) == 0 {
		return "", nil // No log files found
	}

	// Sort to get most recent (lexicographic sort works with YYYYMMDD_HHMMSS format)
	sort.Strings(matches)
	logFile := matches[len(matches)-1] // Most recent

	// Check if log file exists first
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		// Log file doesn't exist yet - return empty result, not an error
		return "", nil
	}

	// Append the log file path to the grep arguments
	args := append(grepArgs, logFile)

	// Execute grep command
	cmd := exec.Command("grep", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		// exit status 1: no match found
		// exit status 2: file not found or other error
		// Both are not considered errors for dgrep, just return empty output
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 || exitError.ExitCode() == 2 {
				return "", nil // Return empty string, no error
			}
		}
		// Any other error is a real failure
		return "", fmt.Errorf("command execution failed: %v", err)
	}

	// Success
	return string(output), nil
}

// getMachineNumber parses the node number from a hostname.
// Supports both Docker-style hostnames (e.g., 'node1', 'node10')
// and legacy numeric suffixes.
func getMachineNumber(hostname string) (int, error) {
	// Try "node<N>" format first (Docker containers)
	re := regexp.MustCompile(`node(\d+)`)
	match := re.FindStringSubmatch(hostname)
	if len(match) >= 2 {
		num, err := strconv.Atoi(match[1])
		if err == nil {
			return num, nil
		}
	}

	// Fallback: try to extract trailing digits
	re2 := regexp.MustCompile(`(\d+)$`)
	match2 := re2.FindStringSubmatch(hostname)
	if len(match2) >= 2 {
		num, err := strconv.Atoi(match2[1])
		if err == nil {
			return num, nil
		}
	}

	return 0, fmt.Errorf("no machine number found in hostname: %s", hostname)
}
