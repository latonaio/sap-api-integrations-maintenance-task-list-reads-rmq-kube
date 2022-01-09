package sap_api_caller

import (
	"fmt"
	"io/ioutil"
	"net/http"
	sap_api_output_formatter "sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Output_Formatter"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library/logger"
	"golang.org/x/xerrors"
)

type RMQOutputter interface {
	Send(sendQueue string, payload map[string]interface{}) error
}

type SAPAPICaller struct {
	baseURL      string
	apiKey       string
	outputQueues []string
	outputter    RMQOutputter
	log          *logger.Logger
}

func NewSAPAPICaller(baseUrl string, outputQueueTo []string, outputter RMQOutputter, l *logger.Logger) *SAPAPICaller {
	return &SAPAPICaller{
		baseURL:      baseUrl,
		apiKey:       GetApiKey(),
		outputQueues: outputQueueTo,
		outputter:    outputter,
		log:          l,
	}
}

func (c *SAPAPICaller) AsyncGetMaintenanceTaskList(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter, equipment, plant, taskListSequence, maintenancePackageText, technicalObject, operationText string, accepter []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(accepter))
	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				c.Header(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter)
				wg.Done()
			}()
		case "HeaderEquipmentPlant":
			func() {
				c.HeaderEquipmentPlant(equipment, plant)
				wg.Done()
			}()
		case "StrategyPackage":
			func() {
				c.StrategyPackage(taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
				wg.Done()
			}()
		case "StrategyPackageText":
			func() {
				c.StrategyPackageText(maintenancePackageText)
				wg.Done()
			}()
		case "Operation":
			func() {
				c.Operation(plant, technicalObject)
				wg.Done()
			}()
		case "OperationText":
			func() {
				c.OperationText(plant, operationText)
				wg.Done()
			}()
		case "OperationMaterial":
			func() {
				c.OperationMaterial(taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
				wg.Done()
			}()
		default:
			wg.Done()
		}
	}

	wg.Wait()
}

func (c *SAPAPICaller) Header(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementHeader("MaintenanceTaskList", taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListHeader"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementHeader(api, taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter string) ([]sap_api_output_formatter.Header, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithHeader(req, taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToHeader(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) HeaderEquipmentPlant(equipment, plant string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementHeaderEquipmentPlant("MaintenanceTaskList", equipment, plant)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListHeader"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementHeaderEquipmentPlant(api, equipment, plant string) ([]sap_api_output_formatter.Header, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithHeaderEquipmentPlant(req, equipment, plant)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToHeader(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) StrategyPackage(taskListType, taskListGroup, taskListGroupCounter, taskListSequence string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementStrategyPackage("MaintenanceTaskListStrtgyPckg", taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListStrategyPackage"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementStrategyPackage(api, taskListType, taskListGroup, taskListGroupCounter, taskListSequencet string) ([]sap_api_output_formatter.StrategyPackage, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithStrategyPackage(req, taskListType, taskListGroup, taskListGroupCounter, taskListSequencet)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToStrategyPackage(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) StrategyPackageText(maintenancePackageText string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementStrategyPackageText("MaintenanceTaskListStrtgyPckg", maintenancePackageText)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListStrategyPackage"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementStrategyPackageText(api, maintenancePackageText string) ([]sap_api_output_formatter.StrategyPackage, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithStrategyPackageText(req, maintenancePackageText)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToStrategyPackage(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) Operation(plant, technicalObject string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementOperation("MaintenanceTaskListOperation", plant, technicalObject)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListOperation"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementOperation(api, plant, technicalObject string) ([]sap_api_output_formatter.Operation, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithOperation(req, plant, technicalObject)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToOperation(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) OperationText(plant, operationText string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementOperationText("MaintenanceTaskListOperation", plant, operationText)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListOperation"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementOperationText(api, plant, operationText string) ([]sap_api_output_formatter.Operation, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithOperationText(req, plant, operationText)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToOperation(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) OperationMaterial(taskListType, taskListGroup, taskListGroupCounter, taskListSequence string) {
	data, err := c.callMaintenanceTaskListSrvAPIRequirementOperationMaterial("MaintenanceTaskListOpMat", taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
	if err != nil {
		c.log.Error(err)
		return
	}
	err = c.outputter.Send(c.outputQueues[0], map[string]interface{}{"message": data, "function": "MaintenanceTaskListOperationMaterial"})
	if err != nil {
		c.log.Error(err)
		return
	}
	c.log.Info(data)
}

func (c *SAPAPICaller) callMaintenanceTaskListSrvAPIRequirementOperationMaterial(api, taskListType, taskListGroup, taskListGroupCounter, taskListSequence string) ([]sap_api_output_formatter.OperationMaterial, error) {
	url := strings.Join([]string{c.baseURL, "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001", api}, "/")
	req, _ := http.NewRequest("GET", url, nil)

	c.setHeaderAPIKeyAccept(req)
    c.getQueryWithOperationMaterial(req, taskListType, taskListGroup, taskListGroupCounter, taskListSequence)

	resp, err := new(http.Client).Do(req)
	if err != nil {
		return nil, xerrors.Errorf("API request error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, xerrors.Errorf("API status code %d. API request failed", resp.StatusCode)
	}

	byteArray, _ := ioutil.ReadAll(resp.Body)
	data, err := sap_api_output_formatter.ConvertToOperationMaterial(byteArray, c.log)
	if err != nil {
		return nil, xerrors.Errorf("convert error: %w", err)
	}
	return data, nil
}

func (c *SAPAPICaller) setHeaderAPIKeyAccept(req *http.Request) {
	req.Header.Set("APIKey", c.apiKey)
	req.Header.Set("Accept", "application/json")
}

func (c *SAPAPICaller) getQueryWithHeader(req *http.Request, taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("TaskListType eq '%s' and TaskListGroup eq '%s' and TaskListGroupCounter eq '%s' and TaskListVersionCounter eq '%s'", taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithHeaderEquipmentPlant(req *http.Request, equipment, plant string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("Equipment eq '%s' and Plant eq '%s'", equipment, plant))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithStrategyPackage(req *http.Request, taskListType, taskListGroup, taskListGroupCounter, taskListSequence string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("TaskListType eq '%s' and TaskListGroup eq '%s' and TaskListGroupCounter eq '%s' and TaskListSequence eq '%s'", taskListType, taskListGroup, taskListGroupCounter, taskListSequence))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithStrategyPackageText(req *http.Request, maintenancePackageText string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("substringof('%s', MaintenancePackageText)", maintenancePackageText))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithOperation(req *http.Request, plant, technicalObject string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("Plant eq '%s' and TechnicalObject eq '%s'", plant, technicalObject))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithOperationText(req *http.Request, plant, operationText string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("Plant eq '%s' and substringof('%s', OperationText)", plant, operationText))
	req.URL.RawQuery = params.Encode()
}

func (c *SAPAPICaller) getQueryWithOperationMaterial(req *http.Request, taskListType, taskListGroup, taskListGroupCounter, taskListSequence string) {
	params := req.URL.Query()
	params.Add("$filter", fmt.Sprintf("TaskListType eq '%s' and TaskListGroup eq '%s' and TaskListGroupCounter eq '%s' and TaskListSequence eq '%s'", taskListType, taskListGroup, taskListGroupCounter, taskListSequence))
	req.URL.RawQuery = params.Encode()
}
