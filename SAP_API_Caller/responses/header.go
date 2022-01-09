package responses

type Header struct {
	Value             []struct {
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
	} `json:"value"`
}
