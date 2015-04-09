// Copyright 2014 The DevMine Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"labix.org/v2/mgo/bson"

	_ "github.com/lib/pq"
)

// GitHub entities
const (
	ghUsers             = "users"
	ghOrgMembers        = "org_members"
	ghRepos             = "repos"
	ghRepoCollaborators = "repo_collaborators"
)

// GHTorrent structures for unmarshalling BSON.
type (
	// ghUser represents a GitHub user.
	ghUser struct {
		ID        int64  `bson:"id"`
		Login     string `bson:"login"`
		AvatarURL string `bson:"avatar_url"`
		HTMLURL   string `bson:"html_url"`
		Type      string `bson:"type"` // User or Organization
		Name      string `bson:"name"` // Real name
		Company   string `bson:"company"`
		Bio       string `bson:"bio"`
		Location  string `bson:"location"`
		Email     string `bson:"email"`
		Hireable  bool   `bson:"hireable"`
		Followers int64  `bson:"followers"`
		Following int64  `bson:"following"`
		CreatedAt string `bson:"created_at"`
		UpdatedAt string `bson:"updated_at"`
	}

	// ghOrgMember is a relation between an organization and a user.
	ghOrgMember struct {
		ID    int64  `bson:"id"`
		Login string `bson:"login"`
		Org   string `bson:"org"`
		Type  string `bson:"type"` // should always be "User"
	}

	// ghRepo represents a GitHub repository.
	ghRepo struct {
		ID               int64  `bson:"id"`
		Name             string `bson:"name"`
		FullName         string `bson:"full_name"`
		Description      string `bson:"description"`
		Homepage         string `bson:"homepage"`
		Language         string `bson:"language"`
		DefaultBranch    string `bson:"default_branch"`
		MasterBranch     string `bson:"master_branch"`
		HTMLURL          string `bson:"html_url"`
		CloneURL         string `bson:"clone_url"`
		Fork             bool   `bson:"fork"`
		ForksCount       int64  `bson:"forks_count"`
		OpenIssuesCount  int64  `bson:"open_issues_count"`
		StargazersCount  int64  `bson:"stargazers_count"`
		SubscribersCount int64  `bson:"subscribers_count"`
		WatchersCount    int64  `bson:"watchers_count"`
		SizeInKb         int64  `bson:"size_in_kb"`
		CreatedAt        string `bson:"created_at"`
		UpdatedAt        string `bson:"updated_at"`
		PushedAt         string `bson:"pushed_at"`

		// Repository owner
		Owner struct {
			Login string `bson:"login"`
		} `bson:"owner"`
	}

	// ghRepoCollaborator is a relation between a user and a repository.
	ghRepoCollaborator struct {
		ID    int64  `bson:"id"`
		Login string `bson:"login"`
		Repo  string `bson:"repo"`
		Owner string `bson:"owner"`
	}
)

// Tables fields
var (
	usersFields   = []string{"username", "name", "email"}
	ghUsersFields = []string{
		"user_id",
		"github_id",
		"login",
		"bio",
		"company",
		"email",
		"hireable",
		"location",
		"avatar_url",
		"html_url",
		"followers_count",
		"following_count",
		"created_at",
		"updated_at",
	}
	ghOrgsFields = []string{
		"login",
		"github_id",
		"avatar_url",
		"html_url",
		"name",
		"company",
		"location",
		"email",
		"created_at",
		"updated_at",
	}
	reposFields   = []string{"name", "primary_language", "clone_url", "clone_path", "vcs"}
	ghReposFields = []string{
		"repository_id",
		"full_name",
		"description",
		"homepage",
		"fork",
		"github_id",
		"default_branch",
		"master_branch",
		"html_url",
		"forks_count",
		"open_issues_count",
		"stargazers_count",
		"subscribers_count",
		"watchers_count",
		"created_at",
		"updated_at",
		"pushed_at",
	}
	reposCollabosFields = []string{"user_id", "repository_id"}
	orgMembersFields    = []string{"gh_user_id", "gh_organization_id"}
)

// config holds ght2dm configuration.
type config struct {
	// BSON files folders. The order is kept while processing them.
	//
	// The name of each folder MUST match the name of the GitHub entity in
	// snake case and pluralized (see defined constants).
	GHTorrentFolder []string `json:"ghtorrent_folders"`

	// database config
	DevMineDatabase devmineDatabase `json:"devmine_database"`
}

// devmineDatabase holds database login information.
//
// PostgreSQL is only database supported for now.
type devmineDatabase struct {
	Host     string `json:"host"`     // host where the database is running
	Port     int    `json:"port"`     // database port
	User     string `json:"user"`     // database user
	Password string `json:"password"` // user's password
	Database string `json:"database"` // DevMine database
	SSLMode  string `json:"ssl_mode"` // SSL mode (disable, require)
}

// readConfig reads the configuration file and parses it.
func readConfig(path string) (*config, error) {
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := config{}
	if err := json.Unmarshal(bs, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// dumpReader is a reader for BSON files.
type dumpReader struct {
	r io.Reader
}

// newDumpReader creates a new dumpReader that reads from r.
func newDumpReader(r io.Reader) *dumpReader {
	return &dumpReader{r: r}
}

// ReadDoc reads the next BSON document.
func (dr *dumpReader) ReadDoc() ([]byte, error) {
	lenBuf := make([]byte, 4)
	if n, err := dr.r.Read(lenBuf); err != nil {
		return nil, err
	} else if n != 4 {
		return nil, errors.New("malformed bson dump")
	}

	var docLen int32
	if err := binary.Read(bytes.NewReader(lenBuf), binary.LittleEndian, &docLen); err != nil {
		return nil, err
	}

	doc := make([]byte, docLen)

	// We copy the length field into the document buffer because it is part of
	// the document and it is expected by bson.Unmarshal.
	copy(doc, lenBuf)

	if _, err := dr.r.Read(doc[4:]); err != nil {
		return nil, err
	}

	return doc, nil
}

// importUsers imports a BSON file containing GitHub users into the DevMine
// database.
func importUsers(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := newDumpReader(f)

	// Begin a new transaction.
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	// Disable foreign key constraints.
	_, err = txn.Exec("ALTER TABLE ONLY gh_users DROP CONSTRAINT gh_users_fk_users")
	if err != nil {
		return err
	}

	userStmt, err := txn.Prepare(genInsQuery("users", usersFields...) + " RETURNING id")
	if err != nil {
		return err
	}

	ghUserStmt, err := txn.Prepare(genInsQuery("gh_users", ghUsersFields...))
	if err != nil {
		return err
	}

	ghOrgStmt, err := txn.Prepare(genInsQuery("gh_organizations", ghOrgsFields...))
	if err != nil {
		return err
	}

	for {
		bs, err := r.ReadDoc()
		if err == io.EOF {
			break
		} else if err != nil {
			fail(err)
			continue
		}

		ghu := ghUser{}
		if err := bson.Unmarshal(bs, &ghu); err != nil {
			fail(path, ":", err)
			continue
		}

		printVerbose("importing gh_user with login", ghu.Login)

		switch ghu.Type {
		case "User":
			userID, err := insertUser(txn, userStmt, ghu)
			if err != nil {
				fail(err)
				continue
			}

			if err := insertGhUser(txn, ghUserStmt, ghu, userID); err != nil {
				fail(err)
				continue
			}
		case "Organization":
			if err := insertGhOrg(txn, ghOrgStmt, ghu); err != nil {
				fail(err)
				continue
			}
		default: // should never happen
			fail(fmt.Errorf("invalid type of user %s", ghu.Type))
			continue
		}
	}

	if err := userStmt.Close(); err != nil {
		return err
	}
	if err := ghUserStmt.Close(); err != nil {
		return err
	}
	if err := ghOrgStmt.Close(); err != nil {
		return err
	}

	// Re-enable foreign key constraints.
	_, err = txn.Exec("ALTER TABLE ONLY gh_users ADD CONSTRAINT gh_users_fk_users FOREIGN KEY (user_id) REFERENCES users(id)")
	if err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

// insertGhOrg inserts a GitHub organization into the database.
func insertGhOrg(txn *sql.Tx, stmt *sql.Stmt, ghu ghUser) error {
	if id := fetchOrgID(txn, ghu.ID); id != 0 {
		if id == -1 {
			return errors.New("impossible to insert github organization with login = " + ghu.Login)
		}
		return nil
	}

	// Some documents only have a creation date, so for these ones, we set the
	// last modification date to the creation date.
	if ghu.UpdatedAt == "" {
		ghu.UpdatedAt = ghu.CreatedAt
	}

	_, err := stmt.Exec(
		ghu.Login,
		ghu.ID,
		ghu.AvatarURL,
		ghu.HTMLURL,
		ghu.Name,
		ghu.Company,
		ghu.Location,
		ghu.Email,
		ghu.CreatedAt,
		ghu.UpdatedAt)
	if err != nil {
		fail(err)
		return errors.New("impossible to insert github organization with login = " + ghu.Login)
	}
	return nil
}

// insertGhUser inserts a GitHub user into the database.
func insertGhUser(txn *sql.Tx, stmt *sql.Stmt, ghu ghUser, userID int64) error {
	if id := fetchGhUserID(txn, ghu.ID); id != 0 {
		if id == -1 {
			return errors.New("impossible to insert github user with login = " + ghu.Login)
		}
		return nil
	}

	// Some documents only have a creation date, so for these ones, we set the
	// last modification date to the creation date.
	if ghu.UpdatedAt == "" {
		ghu.UpdatedAt = ghu.CreatedAt
	}

	_, err := stmt.Exec(
		userID,
		ghu.ID,
		ghu.Login,
		ghu.Bio,
		ghu.Company,
		ghu.Email,
		ghu.Hireable,
		ghu.Location,
		ghu.AvatarURL,
		ghu.HTMLURL,
		ghu.Followers,
		ghu.Following,
		ghu.CreatedAt,
		ghu.UpdatedAt)
	if err != nil {
		fail(err)
		return errors.New("impossible to insert github user with login = " + ghu.Login)
	}
	return nil
}

// insertUser inserts a user into the database.
func insertUser(txn *sql.Tx, stmt *sql.Stmt, ghu ghUser) (int64, error) {
	if id := fetchUserID(txn, ghu.ID); id != 0 {
		if id == -1 {
			return 0, errors.New("impossible to insert user with login " + ghu.Login)
		}
		return 0, nil
	}

	var userID int64
	err := stmt.QueryRow(ghu.Login, ghu.Name, ghu.Email).Scan(&userID)
	if err != nil {
		fail(err)
		return 0, errors.New("impossible to insert user with login " + ghu.Login)
	}
	return userID, nil
}

// fetchUserID fetches the user ID corresponding to a given GitHub user ID.
//
// It returns 0 if the user does not already exists in the database and -1 if
// an error occured while processing the query.
//
// When an error occurs, this function takes care of logging it before
// returning -1.
func fetchUserID(txn *sql.Tx, githubID int64) int64 {
	var id int64
	err := txn.QueryRow("SELECT user_id FROM gh_users WHERE github_id=$1", githubID).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail("failed to fetch user id: ", err)
		return -1
	}

	return id
}

// fetchGhUserID fetches the GitHub user ID corresponding to a given GitHub user
// ID.
// It returns 0 if the GitHub user does not already exists in the database and
// -1 if an error occured while processing the query.
func fetchGhUserID(txn *sql.Tx, githubID int64) int64 {
	var id int64
	err := txn.QueryRow("SELECT id FROM gh_users WHERE github_id=$1", githubID).Scan(&id)
	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail("failed to fetch github user id: ", err)
		return -1
	}

	return id
}

// fetchOrgID fetches the organizationID corresponding to a given GitHub user
// ID.
// It returns 0 if the organization does not already exists in the database and
// -1 if an error occured while processing the query.
func fetchOrgID(txn *sql.Tx, githubID int64) int64 {
	var id int64
	err := txn.QueryRow("SELECT id FROM gh_organizations WHERE github_id=$1", githubID).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail("failed to fetch organization id: ", err)
		return -1
	}

	return id
}

// importRepos imports a BSON file containing GitHub repositories into the
// DevMine database.
func importRepos(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := newDumpReader(f)

	// Begin a new transaction.
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	// Disable foreign key constraints.
	_, err = txn.Exec("ALTER TABLE ONLY gh_repositories DROP CONSTRAINT gh_repositories_fk_repositories")
	if err != nil {
		return err
	}

	repoStmt, err := txn.Prepare(genInsQuery("repositories", reposFields...) + " RETURNING id")
	if err != nil {
		return err
	}
	ghRepoStmt, err := txn.Prepare(genInsQuery("gh_repositories", ghReposFields...))
	if err != nil {
		return err
	}

	for {
		bs, err := r.ReadDoc()
		if err == io.EOF {
			break
		} else if err != nil {
			fail(err)
			continue
		}

		ghr := ghRepo{}
		if err := bson.Unmarshal(bs, &ghr); err != nil {
			fail(err)
			continue
		}

		printVerbose("importing gh_repo with clone url", ghr.HTMLURL+".git")

		repoID, err := insertRepo(txn, repoStmt, ghr)
		if err != nil {
			fail(err)
			continue
		}

		if err := insertGhRepo(txn, ghRepoStmt, ghr, repoID); err != nil {
			fail(err)
			continue
		}
	}

	if err := repoStmt.Close(); err != nil {
		return err
	}
	if err := ghRepoStmt.Close(); err != nil {
		return err
	}

	// Re-enable foreign key constraints.
	_, err = txn.Exec("ALTER TABLE ONLY gh_repositories ADD CONSTRAINT gh_repositories_fk_repositories FOREIGN KEY (repository_id) REFERENCES repositories(id)")
	if err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

// insertRepo inserts a repository into the database.
func insertRepo(txn *sql.Tx, stmt *sql.Stmt, ghr ghRepo) (int64, error) {
	if id := fetchRepoID(txn, ghr.ID); id != 0 {
		if id == -1 {
			return 0, fmt.Errorf("impossible to insert repository with id %d", ghr.ID)
		}
		return 0, nil
	}

	clonePath := strings.ToLower(filepath.Join(ghr.Language, ghr.Owner.Login, ghr.Name))

	var repoID int64
	err := stmt.QueryRow(ghr.Name, ghr.Language, ghr.CloneURL, clonePath, "git").Scan(&repoID)
	if err != nil {
		fail(err)
		return 0, fmt.Errorf("impossible to insert repository with id %d", ghr.ID)
	}
	return repoID, nil
}

// insertGhRepo inserts a  GitHub repository into the database.
func insertGhRepo(txn *sql.Tx, stmt *sql.Stmt, ghr ghRepo, repoID int64) error {
	if id := fetchRepoID(txn, ghr.ID); id != 0 {
		if id == -1 {
			return fmt.Errorf("impossible to insert github repository with id %d", ghr.ID)
		}
		return nil
	}

	_, err := stmt.Exec(
		repoID,
		ghr.FullName,
		ghr.Description,
		ghr.Homepage,
		ghr.Fork,
		ghr.ID,
		ghr.DefaultBranch,
		ghr.MasterBranch,
		ghr.HTMLURL,
		ghr.ForksCount,
		ghr.OpenIssuesCount,
		ghr.StargazersCount,
		ghr.SubscribersCount,
		ghr.WatchersCount,
		ghr.CreatedAt,
		ghr.UpdatedAt,
		ghr.PushedAt)
	if err != nil {
		fail(err)
		return fmt.Errorf("impossible to insert github repository with id %d", ghr.ID)
	}
	return nil
}

// fetchRepoID fetches the repository ID corresponding to a given GitHub
// repository ID.
//
// It returns 0 if the repository does not already exists in the database and
// -1 if an error occured while processing the query
//
// When an error occurs, this function takes care of logging it before
// returning -1.
func fetchRepoID(txn *sql.Tx, githubID int64) int64 {
	var id int64
	err := txn.QueryRow("SELECT repository_id FROM gh_repositories WHERE github_id=$1", githubID).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail("failed to fetch repository id: ", err)
		return -1
	}

	return id
}

// importOrgMembers imports a BSon file containing GitHub organization members
// into the DevMine database.
func importOrgMembers(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := newDumpReader(f)

	// Begin a new transaction.
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	orgMemberStmt, err := txn.Prepare(genInsQuery("gh_users_organizations", orgMembersFields...))
	if err != nil {
		return err
	}

	for {
		bs, err := r.ReadDoc()
		if err == io.EOF {
			break
		} else if err != nil {
			fail(err)
			continue
		}

		ghom := ghOrgMember{}
		if err := bson.Unmarshal(bs, &ghom); err != nil {
			fail(err)
			continue
		}

		if err := insertOrgMember(txn, orgMemberStmt, ghom); err != nil {
			fail(err)
			continue
		}
	}

	if err := orgMemberStmt.Close(); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// insertOrgMember inserts a GitHub organization member into the database.
func insertOrgMember(txn *sql.Tx, stmt *sql.Stmt, ghom ghOrgMember) error {
	rows, err := txn.Query(`
		SELECT *
		FROM gh_users_organizations
		LEFT JOIN gh_users ON gh_users.id = gh_users_organizations.gh_user_id
		LEFT JOIN gh_organizations ON gh_organizations.id = gh_users_organizations.gh_organization_id
		WHERE gh_users.login = $1 AND gh_organizations.login = $2
	`, ghom.Login, ghom.Org)
	defer rows.Close()

	switch {
	case rows.Next():
		return nil // the relation already exist, no need to create it
	case err != nil:
		fail(err)
		return fmt.Errorf("impossible to fetch member organization with id %d", ghom.ID)
	default:
		break // the relation does not already exist, so we can create it
	}

	ghUserID := fetchGhUserIDFromLogin(txn, ghom.Login)
	if ghUserID <= 0 {
		return fmt.Errorf("failed to retrieve the id of the github user having the login %s", ghom.Login)
	}

	ghOrgID := fetchGhOrgIDFromLogin(txn, ghom.Org)
	if ghOrgID <= 0 {
		return fmt.Errorf("failed to retrieve the id of the github organization having the login %s", ghom.Org)
	}

	if _, err = stmt.Exec(ghUserID, ghOrgID); err != nil {
		fail(err)
		return fmt.Errorf("impossible to insert member organization with id %d", ghom.ID)
	}
	return nil
}

// fetchGhUserIDFromLogin fetches the GitHub user ID corresponding to a given
// login.
// It returns 0 if the GitHub user does not already exists in the database and
// -1 if an error occured while processing the query.
func fetchGhUserIDFromLogin(txn *sql.Tx, login string) int64 {
	var id int64
	err := txn.QueryRow("SELECT id FROM gh_users WHERE login=$1", login).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail(fmt.Sprintf("failed to fetch github user with login %s:", login), err)
		return -1
	}

	return id
}

// fetchGhOrgIDFromLogin fetches the GitHub organization ID corresponding to a
// given login.
// It returns 0 if the GitHub organization does not already exists in the
// database and -1 if an error occured while processing the query.
func fetchGhOrgIDFromLogin(txn *sql.Tx, login string) int64 {
	var id int64
	err := txn.QueryRow("SELECT id FROM gh_organizations WHERE login=$1", login).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail(fmt.Sprintf("failed to fetch github organization with login %s:", login), err)
		return -1
	}

	return id
}

// importRepoCollabo imports a BSON file containing GitHub repository
// collaborators into the DevMine database.
func importRepoCollabo(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := newDumpReader(f)

	// Begin a new transaction.
	txn, err := db.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	repoCollaboStmt, err := txn.Prepare(genInsQuery("users_repositories", reposCollabosFields...))
	if err != nil {
		return err
	}

	for {
		bs, err := r.ReadDoc()
		if err == io.EOF {
			break
		} else if err != nil {
			fail(err)
			continue
		}

		ghrc := ghRepoCollaborator{}
		if err := bson.Unmarshal(bs, &ghrc); err != nil {
			fail(err)
			continue
		}

		printVerbose("importing repo_collaborators with login", ghrc.Login, ", owner", ghrc.Owner, "and repo", ghrc.Repo)

		if err := insertRepoCollabo(txn, repoCollaboStmt, ghrc); err != nil {
			fail(err)
			continue
		}
	}

	if err := repoCollaboStmt.Close(); err != nil {
		return err
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

// insertRepoCollabo inserts a GitHub repository collaborator into the database.
func insertRepoCollabo(txn *sql.Tx, stmt *sql.Stmt, ghrc ghRepoCollaborator) error {
	rows, err := txn.Query(`
		SELECT users_repositories.user_id, users_repositories.repository_id
		FROM users_repositories
		LEFT JOIN users ON users.id = users_repositories.user_id
		LEFT JOIN gh_users ON gh_users.user_id = users.id
		LEFT JOIN repositories ON repositories.id = users_repositories.repository_id
		LEFT JOIN gh_repositories ON gh_repositories.id = repositories.id
		WHERE gh_users.login = $1 AND gh_repositories.full_name = $2
	`, ghrc.Login, ghrc.Owner+"/"+ghrc.Repo)
	if rows != nil {
		defer rows.Close()
	}

	switch {
	case rows != nil && rows.Next():
		var userID, repoID int64
		if err := rows.Scan(&userID, &repoID); err == nil {
			printVerbose(fmt.Sprintf("the users_repositories relation (%d, %d) already exists", userID, repoID))
		}
		return nil // the relation already exist, no need to create it
	case err != nil:
		fail(err)
		return fmt.Errorf("impossible to fetch repo collaborator with id %d", ghrc.ID)
	default:
		break // the relation does not already exist, so we can create it
	}

	ghUserID := fetchGhUserIDFromLogin(txn, ghrc.Login)
	if ghUserID <= 0 {
		return fmt.Errorf("failed to retrieve github user id with login %s", ghrc.Login)
	}

	ghRepoID := fetchRepoIDFromFullname(txn, ghrc.Owner+"/"+ghrc.Repo)
	if ghRepoID <= 0 {
		return fmt.Errorf("failed to retrieve github repository id with login %s", ghrc.Login)
	}

	if _, err = stmt.Exec(ghUserID, ghRepoID); err != nil {
		fail(err)
		return fmt.Errorf("impossible to fetch insert repository collaborator with id %d", ghrc.ID)
	}
	return nil
}

// fetchRepoIDFromFullname fetches the repository ID corresponding to a
// given GitHub repository fullname.
// It returns 0 if the repository does not already exists in the
// database and -1 if an error occured while processing the query.
func fetchRepoIDFromFullname(txn *sql.Tx, fullname string) int64 {
	var id int64
	err := txn.QueryRow(`
		SELECT repositories.id AS repo_id
		FROM repositories
		LEFT JOIN gh_repositories ON gh_repositories.repository_id = repositories.id
		WHERE gh_repositories.full_name=$1
	`, fullname).Scan(&id)

	switch {
	case err == sql.ErrNoRows:
		return 0
	case err != nil:
		fail("failed to fetch repository id: ", err)
		return -1
	}

	return id
}

// genInsQuery generates a query string for an insertion into the database.
func genInsQuery(tableName string, fields ...string) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("INSERT INTO %s(%s)\n", tableName, strings.Join(fields, ",")))
	buf.WriteString("VALUES(")

	for ind, _ := range fields {
		if ind > 0 {
			buf.WriteString(",")
		}

		buf.WriteString(fmt.Sprintf("$%d", ind+1))
	}

	buf.WriteString(")\n")

	return buf.String()
}

// A fileInfoList is just a wrapper around a slice of os.FileInfo that
// implements the sort.Interface. In other words, it is a sortable list of
// os.FileInfo. They are sorted by the date (the one present in the file name)
// in descending order.
type fileInfoList []os.FileInfo

func (fil fileInfoList) Len() int {
	return len(fil)
}

func (fil fileInfoList) Swap(i, j int) {
	fil[i], fil[j] = fil[j], fil[i]
}

func (fil fileInfoList) Less(i, j int) bool {
	di, err := time.Parse("2006-01-02", strings.TrimSuffix(fil[i].Name(), ".bson"))
	if err != nil {
		// this should never happen since file must have a correct name
		fail(err)
		return false
	}

	dj, err := time.Parse("2006-01-02", strings.TrimSuffix(fil[j].Name(), ".bson"))
	if err != nil {
		// this should never happen since file must have a correct name
		fail(err)
		return false
	}

	return di.After(dj)
}

func visit(path, entity string) error {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	fil := fileInfoList(fis)
	sort.Sort(fil)

	for _, fi := range fil {
		if ok, err := regexp.MatchString("[0-9]{4}-[0-9]{2}-[0-9]{2}\\.bson", fi.Name()); !ok {
			if err != nil {
				fail(err)
			}
			fmt.Printf("[%s] skipped '%s'\n", entity, fi.Name())
			continue
		}

		fmt.Printf("[%s] processing '%s'\n", entity, fi.Name())

		fullpath := filepath.Join(path, fi.Name())
		var err error

		switch entity {
		case ghUsers:
			if err = importUsers(fullpath); err != nil {
				break
			}
			// Since we are doing a bulk imports, we had to disable constraint key
			// validation and insert the users and gh_users without relation. Thus we
			// have to create the relations now. For database consistency, it is
			// important that this operation not fail.
			/*if err := linkGhUserToUser(); err != nil {
				return fmt.Errorf("failed to link all gh_users to users: %v", err)
			}*/
		case ghOrgMembers:
			err = importOrgMembers(fullpath)
		case ghRepos:
			if err = importRepos(fullpath); err != nil {
				break
			}
			/*if err := linkGhRepoToRepo(); err != nil {
				fail(fmt.Errorf("failed to link all gh_repos to repos: %v", err))
			}*/
		case ghRepoCollaborators:
			err = importRepoCollabo(fullpath)
		}

		if err != nil {
			fail(fmt.Sprintf("failed to import bson '%s': %v",
				filepath.Join(path, fi.Name()), err))
		}
	}

	return nil
}

// fatal log an error into stderr and exit with status 1.
func fatal(a ...interface{}) {
	// XXX: avoid code duplication
	var msg string
	if *dflag {
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		} else {
			file = filepath.Base(file)
		}
		msg = fmt.Sprintf("[%s:%d]", file, line)
	}
	msg += fmt.Sprint(a...)
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// fail log an error without exiting.
func fail(a ...interface{}) {
	var msg string
	if *dflag {
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		} else {
			file = filepath.Base(file)
		}
		msg = fmt.Sprintf("[%s:%d]", file, line)
	}
	msg += fmt.Sprint(a...)
	fmt.Fprintln(os.Stderr, msg)
}

// printVerbose only print messages when the verbose mode is enabled by vflag.
func printVerbose(a ...interface{}) {
	if *vflag {
		fmt.Println(a...)
	}
}

// db is the database session
var db *sql.DB

// setupDB connects to the database and initialize the session.
// This must only be called once and from the main().
func setupDB(cfg devmineDatabase) error {
	dbURL := fmt.Sprintf(
		"user='%s' password='%s' host='%s' port=%d dbname='%s' sslmode='%s'",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode)

	var err error
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		return err
	}

	return nil
}

// Command line options.
var (
	vflag = flag.Bool("v", false, "enable verbose mode")
	dflag = flag.Bool("d", false, "enable debug mode")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [config]\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Available options:")
		flag.PrintDefaults()
		os.Exit(1)
	}
	flag.Parse()

	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "invalid # of arguments")
		flag.Usage()
	}

	cfg, err := readConfig(flag.Arg(0))
	if err != nil {
		fatal(err)
	}

	if err := setupDB(cfg.DevMineDatabase); err != nil {
		fatal(err)
	}
	defer db.Close()

	for _, f := range cfg.GHTorrentFolder {
		if err := visit(f, filepath.Base(f)); err != nil {
			fatal(err)
		}
	}
}
