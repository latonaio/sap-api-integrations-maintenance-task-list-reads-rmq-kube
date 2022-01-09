package responses

type OperationMaterial struct {
	Value             []struct {
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
	} `json:"value"`
}
