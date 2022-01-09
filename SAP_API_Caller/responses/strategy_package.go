package responses

type StrategyPackage struct {
	Value             []struct {
		TaskListType                   string        `json:"TaskListType"`
		TaskListGroup                  string        `json:"TaskListGroup"`
		TaskListGroupCounter           string        `json:"TaskListGroupCounter"`
		TaskListSequence               string        `json:"TaskListSequence"`
		TaskListOperationInternalID    string        `json:"TaskListOperationInternalId"`
		MaintenancePackage             string        `json:"MaintenancePackage"`
		MaintPckgTskListOpAllocIntNmbr string        `json:"MaintPckgTskListOpAllocIntNmbr"`
		MaintenancePackageText         string        `json:"MaintenancePackageText"`
		MaintenanceTaskListOperation   string        `json:"MaintenanceTaskListOperation"`
		OperationText                  string        `json:"OperationText"`
		MaintTaskListSubOperation      string        `json:"MaintTaskListSubOperation"`
		ChangeNumber                   string        `json:"ChangeNumber"`
	} `json:"value"`
}
