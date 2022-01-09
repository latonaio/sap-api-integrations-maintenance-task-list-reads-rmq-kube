package responses

type Operation struct {
	Value             []struct {
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
	} `json:"value"`
}
