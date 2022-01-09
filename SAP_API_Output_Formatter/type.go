package sap_api_output_formatter

type MaintenanceTaskList struct {
	ConnectionKey        string `json:"connection_key"`
	Result               bool   `json:"result"`
	RedisKey             string `json:"redis_key"`
	Filepath             string `json:"filepath"`
	APISchema            string `json:"api_schema"`
	MaintenanceTaskList  string `json:"maintenance_task_list"`
	Deleted              bool   `json:"deleted"`
}

type Header struct {
	TaskListType                string        `json:"TaskListType"`
	TaskListGroup               string        `json:"TaskListGroup"`
	TaskListGroupCounter        string        `json:"TaskListGroupCounter"`
	TaskListVersionCounter      string        `json:"TaskListVersionCounter"`
	MaintenancePlanningPlant    string        `json:"MaintenancePlanningPlant"`
	TaskListStatus              string        `json:"TaskListStatus"`
	TaskListStatusDesc          string        `json:"TaskListStatusDesc"`
	TechnicalObject             string        `json:"TechnicalObject"`
	TaskListUsage               string        `json:"TaskListUsage"`
	TaskListDesc                string        `json:"TaskListDesc"`
	WorkCenter                  string        `json:"WorkCenter"`
	MaintenanceStrategy         string        `json:"MaintenanceStrategy"`
	OperationSystemCondition    string        `json:"OperationSystemCondition"`
	Assembly                    string        `json:"Assembly"`
	ChangeNumber                string        `json:"ChangeNumber"`
	ValidityStartDate           string        `json:"ValidityStartDate"`
	ValidityEndDate             string        `json:"ValidityEndDate"`
	IsMarkedForDeletion         bool          `json:"IsMarkedForDeletion"`
	LastChangeDate              string        `json:"LastChangeDate"`
	CreationDate                string        `json:"CreationDate"`
	Plant                       string        `json:"Plant"`
	ResponsiblePlannerGroup     string        `json:"ResponsiblePlannerGroup"`
	Equipment                   string        `json:"Equipment"`
	FunctionalLocationLabelName string        `json:"FunctionalLocationLabelName"`
	TaskListIsHierarchical      bool          `json:"TaskListIsHierarchical"`
}

type StrategyPackage struct {
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
}

type Operation struct {
	TaskListType                   string        `json:"TaskListType"`
	TaskListGroup                  string        `json:"TaskListGroup"`
	TaskListGroupCounter           string        `json:"TaskListGroupCounter"`
	TaskListSequence               string        `json:"TaskListSequence"`
	TaskListOperationInternalID    string        `json:"TaskListOperationInternalId"`
	TaskListOpBOMItmIntVersCounter string        `json:"TaskListOpBOMItmIntVersCounter"`
	MaintenanceTaskListOperation   string        `json:"MaintenanceTaskListOperation"`
	MaintOperationExecStageCode    string        `json:"MaintOperationExecStageCode"`
	OperationText                  string        `json:"OperationText"`
	OperationControlProfile        string        `json:"OperationControlProfile"`
	WorkCenter                     string        `json:"WorkCenter"`
	Plant                          string        `json:"Plant"`
	Assembly                       string        `json:"Assembly"`
	OperationCalculationControl    string        `json:"OperationCalculationControl"`
	OpPlannedWorkQuantity          float64       `json:"OpPlannedWorkQuantity"`
	OpWorkQuantityUnit             string        `json:"OpWorkQuantityUnit"`
	NumberOfCapacities             int           `json:"NumberOfCapacities"`
	PurchaseOrderQty               float64       `json:"PurchaseOrderQty"`
	PurchaseOrderQuantityUnit      string        `json:"PurchaseOrderQuantityUnit"`
	OperationStandardDuration      float64       `json:"OperationStandardDuration"`
	OperationStandardDurationUnit  string        `json:"OperationStandardDurationUnit"`
	CostCtrActivityType            string        `json:"CostCtrActivityType"`
	MaterialGroup                  string        `json:"MaterialGroup"`
	OpExternalProcessingPrice      float64       `json:"OpExternalProcessingPrice"`
	OpExternalProcessingCurrency   string        `json:"OpExternalProcessingCurrency"`
	CostElement                    string        `json:"CostElement"`
	PurchasingGroup                string        `json:"PurchasingGroup"`
	PurchasingOrganization         string        `json:"PurchasingOrganization"`
	PurchaseContract               string        `json:"PurchaseContract"`
	PurchaseContractItem           string        `json:"PurchaseContractItem"`
	Supplier                       string        `json:"Supplier"`
	ChangeNumber                   string        `json:"ChangeNumber"`
	PurchasingInfoRecord           string        `json:"PurchasingInfoRecord"`
	IsBusinessPurposeCompleted     string        `json:"IsBusinessPurposeCompleted"`
	SupplierAccountGroup           string        `json:"SupplierAccountGroup"`
	TaskListStatus                 string        `json:"TaskListStatus"`
	ResponsiblePlannerGroup        string        `json:"ResponsiblePlannerGroup"`
	MaintenancePlanningPlant       string        `json:"MaintenancePlanningPlant"`
	MaintenancePlannerGroup        string        `json:"MaintenancePlannerGroup"`
	ControllingArea                string        `json:"ControllingArea"`
	CostCenter                     string        `json:"CostCenter"`
	MaintenancePlant               string        `json:"MaintenancePlant"`
	ValidityStartDate              string        `json:"ValidityStartDate"`
	ValidityEndDate                string        `json:"ValidityEndDate"`
	TechnicalObject                string        `json:"TechnicalObject"`
	TechObjIsEquipOrFuncnlLoc      string        `json:"TechObjIsEquipOrFuncnlLoc"`
}

type OperationMaterial struct {
	TaskListType                   string        `json:"TaskListType"`
	TaskListGroup                  string        `json:"TaskListGroup"`
	TaskListGroupCounter           string        `json:"TaskListGroupCounter"`
	TaskListSequence               string        `json:"TaskListSequence"`
	TaskListOperationInternalID    string        `json:"TaskListOperationInternalId"`
	TaskListOpBOMItmInternalID     string        `json:"TaskListOpBOMItmInternalID"`
	TaskListOpBOMItmIntVersCounter string        `json:"TaskListOpBOMItmIntVersCounter"`
	MaintenanceTaskListOperation   string        `json:"MaintenanceTaskListOperation"`
	Material                       string        `json:"Material"`
	MaterialName                   string        `json:"MaterialName"`
	BillOfMaterialItemQuantity     int           `json:"BillOfMaterialItemQuantity"`
	BillOfMaterialItemCategory     string        `json:"BillOfMaterialItemCategory"`
	BillOfMaterialItemUnit         string        `json:"BillOfMaterialItemUnit"`
	ResvnIsMRPRlvtOrPurReqnIsCrted string        `json:"ResvnIsMRPRlvtOrPurReqnIsCrted"`
	MatlCompIsMarkedForBackflush   bool          `json:"MatlCompIsMarkedForBackflush"`
	SafetyRelevantObject           string        `json:"SafetyRelevantObject"`
	BillOfMaterialItemCategoryDesc string        `json:"BillOfMaterialItemCategoryDesc"`
	MatlsPlngRelevancyCodeName     string        `json:"MatlsPlngRelevancyCodeName"`
	SafetyRelevanceActionDesc      string        `json:"SafetyRelevanceActionDesc"`
}
