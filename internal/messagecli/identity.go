package messagecli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func readPrivateFile(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("credential file path must be absolute")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 {
		return nil, errors.New("credential file must be an owner-only regular file (mode 0600 or 0400)")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open credential file")
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || after.Mode().Perm()&0077 != 0 {
		return nil, errors.New("credential file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("cannot read bounded credential file")
	}
	return data, nil
}

func readIdentity(path string) (identity, error) {
	var id identity
	data, err := readPrivateFile(path, 8192)
	if err != nil {
		return id, err
	}
	if json.Unmarshal(data, &id) != nil || !validToken(id.Token) || !validID(id.SessionID) || !validID(id.Turn) || !validID(id.AgentID) {
		return identity{}, errors.New("invalid mailbox identity file")
	}
	if _, err := localEndpoint(id.Endpoint); err != nil {
		return identity{}, err
	}
	return id, nil
}

func enroll(opts options, out io.Writer) error {
	credential, err := readPrivateFile(opts.adminFile, 4097)
	if err != nil {
		return err
	}
	client, err := newClient(opts.endpoint, strings.TrimSpace(string(credential)))
	if err != nil {
		return err
	}
	defer client.close()
	file, err := reserveIdentityFile(opts.output)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		file.Close()
		if !committed {
			os.Remove(opts.output)
		}
	}()
	payload := struct {
		SessionID string `json:"sessionId"`
		Turn      string `json:"turn"`
		AgentID   string `json:"agentId"`
	}{opts.sessionID, opts.turn, opts.agentID}
	data, err := client.post(context.Background(), "/api/mailbox/enroll", payload)
	if err != nil {
		return err
	}
	var response struct {
		Token string `json:"token"`
	}
	if json.Unmarshal(data, &response) != nil || !validToken(response.Token) {
		return errors.New("invalid mailbox enrollment response")
	}
	id := identity{client.endpoint, response.Token, opts.sessionID, opts.turn, opts.agentID}
	if json.NewEncoder(file).Encode(id) != nil || file.Sync() != nil {
		return errors.New("cannot save enrolled identity; enrollment may need to be repeated")
	}
	if file.Close() != nil {
		return errors.New("cannot close enrolled identity file")
	}
	committed = true
	if _, err := fmt.Fprintln(out, `{"enrolled":true}`); err != nil {
		return errors.New("identity saved but cannot write enrollment status")
	}
	return nil
}

func reserveIdentityFile(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("identity output path must be absolute")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errors.New("cannot create identity file; use a new file in an existing private directory")
	}
	return file, nil
}
