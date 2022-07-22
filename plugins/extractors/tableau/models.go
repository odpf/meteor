package tableau

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/pkg/errors"
)

// https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_connectiontype.htm
var connectionTypeMap = map[string]string{
	"sqlserver": "mssql",
}

func mapConnectionTypeToSource(ct string) string {
	s, ok := connectionTypeMap[ct]
	if !ok {
		return ct
	}
	return s
}

type Pagination struct {
	PageNumber     string `json:"pageNumber"`
	PageSize       string `json:"pageSize"`
	TotalAvailable string `json:"totalAvailable"`
}

type Project struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Table struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Schema   string   `json:"schema"`
	FullName string   `json:"fullName"`
	Database Database `json:"database"`
}

type Owner struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Workbook struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	ProjectName    string    `json:"projectName"`
	URI            string    `json:"uri"`
	Description    string    `json:"description"`
	Owner          Owner     `json:"owner"`
	Sheets         []*Sheet  `json:"sheets"`
	UpstreamTables []*Table  `json:"upstreamTables"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type Sheet struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html
type DatabaseInterface interface {
	CreateResource(tableInfo Table) (resource *v1beta2.Resource)
}

type Database map[string]interface{}

type DatabaseServer struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	HostName       string `json:"hostName"`
	Port           int    `json:"port"`
	Service        string `json:"service"`
}

// parseBQTableFullName would parse table full name into splitted strings (project id, dataset, table name)
// Full name found in tableau with biqquery source is like this
// sometimes table name can also be the same as full name (e.g. project-id.schema.table1)
// `project-id.schema.table1`
// [project-id.schema].[table1]`
func parseBQTableFullName(fullName string) (splittedFN []string, err error) {
	omitedChars := "`" + "\\[\\]"
	re := regexp.MustCompile("[" + omitedChars + "]")
	cleanedFN := re.ReplaceAllString(fullName, "")
	splittedFN = strings.Split(cleanedFN, ".")
	if len(splittedFN) == 3 {
		return
	}
	err = errors.New("unexpected length of splitted full name")
	return
}

func (dbs *DatabaseServer) CreateResource(tableInfo Table) (resource *v1beta2.Resource) {
	source := mapConnectionTypeToSource(dbs.ConnectionType)

	var urn string
	switch source {
	case "bigquery":
		// bigquery::sample-project/dataset_a/invoice
		// sometimes table name can be the same as full name (e.g. project-id.schema.table1), so we build URN with the full name instead
		fullNameSplitted, err := parseBQTableFullName(tableInfo.FullName)
		if err != nil {
			// assume fullNameSplitted[0] is the project ID
			urn = models.TableURN(source, fullNameSplitted[0], tableInfo.Schema, tableInfo.Name)
			break
		}
		urn = models.TableURN(source, fullNameSplitted[0], fullNameSplitted[1], fullNameSplitted[2])
	default:
		// postgres::postgres:5432/postgres/user
		host := fmt.Sprintf("%s:%d", dbs.HostName, dbs.Port)
		urn = models.TableURN(source, host, dbs.Name, tableInfo.Name)

	}
	resource = &v1beta2.Resource{
		Urn:     urn,
		Type:    "table",
		Service: source,
	}
	return
}

type CloudFile struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	Provider       string `json:"provider"`
	FileExtension  string `json:"fileExtension"`
	MimeType       string `json:"mimeType"`
	RequestURL     string `json:"requestUrl"`
}

func (cf *CloudFile) CreateResource(tableInfo Table) (resource *v1beta2.Resource) {
	source := mapConnectionTypeToSource(cf.ConnectionType)
	urn := fmt.Sprintf("%s::%s/%s/%s", source, cf.Provider, cf.Name, tableInfo.Name)
	resource = &v1beta2.Resource{
		Urn:     urn,
		Type:    "bucket", // TODO need to check what would be the appropriate type for this
		Service: source,
	}
	return
}

type File struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	FilePath       string `json:"filePath"`
}

func (f *File) CreateResource(tableInfo Table) (resource *v1beta2.Resource) {
	source := mapConnectionTypeToSource(f.ConnectionType)
	urn := fmt.Sprintf("%s::%s/%s/%s", source, f.FilePath, f.Name, tableInfo.Name)
	resource = &v1beta2.Resource{
		Urn:     urn,
		Type:    "bucket", // TODO need to check what would be the appropriate type for this
		Service: source,
	}
	return
}

type WebDataConnector struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	ConnectorURL   string `json:"connectorUrl"`
}

func (wdc *WebDataConnector) CreateResource(tableInfo Table) (resource *v1beta2.Resource) {
	source := mapConnectionTypeToSource(wdc.ConnectionType)
	urn := fmt.Sprintf("%s::%s/%s/%s", source, wdc.ConnectorURL, wdc.Name, tableInfo.Name)
	resource = &v1beta2.Resource{
		Urn:     urn,
		Type:    "table", // TODO need to check what would be the appropriate type for this
		Service: source,
	}
	return
}
