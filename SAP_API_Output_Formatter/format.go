package sap_api_output_formatter

import (
	"encoding/json"
	"sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Caller/responses"

	"github.com/latonaio/golang-logging-library/logger"
	"golang.org/x/xerrors"
)

func ConvertToHeader(raw []byte, l *logger.Logger) ([]Header, error) {
	pm := &responses.Header{}
	err := json.Unmarshal(raw, pm)
	if err != nil {
		return nil, xerrors.Errorf("cannot convert to Header. unmarshal error: %w", err)
	}
	if len(pm.Value) == 0 {
		return nil, xerrors.New("Result data is not exist")
	}
	if len(pm.Value) > 10 {
		l.Info("raw data has too many Results. %d Results exist. show the first 10 of Results array", len(pm.Value))
	}

	header := make([]Header, 0, 10)
	for i := 0; i < 10 && i < len(pm.Value); i++ {
		data := pm.Value[i]
		header = append(header, Header{
	TaskListType:                data.TaskListType,
	TaskListGroup:               data.TaskListGroup,
	TaskListGroupCounter:        data.TaskListGroupCounter,
	TaskListVersionCounter:      data.TaskListVersionCounter,
	MaintenancePlanningPlant:    data.MaintenancePlanningPlant,
	TaskListStatus:              data.TaskListStatus,
	TaskListStatusDesc:          data.TaskListStatusDesc,
	TechnicalObject:             data.TechnicalObject,
	TaskListUsage:               data.TaskListUsage,
	TaskListDesc:                data.TaskListDesc,
	WorkCenter:                  data.WorkCenter,
	MaintenanceStrategy:         data.MaintenanceStrategy,
	OperationSystemCondition:    data.OperationSystemCondition,
	Assembly:                    data.Assembly,
	ChangeNumber:                data.ChangeNumber,
	ValidityStartDate:           data.ValidityStartDate,
	ValidityEndDate:             data.ValidityEndDate,
	IsMarkedForDeletion:         data.IsMarkedForDeletion,
	LastChangeDate:              data.LastChangeDate,
	CreationDate:                data.CreationDate,
	Plant:                       data.Plant,
	ResponsiblePlannerGroup:     data.ResponsiblePlannerGroup,
	Equipment:                   data.Equipment,
	FunctionalLocationLabelName: data.FunctionalLocationLabelName,
	TaskListIsHierarchical:      data.TaskListIsHierarchical,
		})
	}

	return header, nil
}

func ConvertToStrategyPackage(raw []byte, l *logger.Logger) ([]StrategyPackage, error) {
	pm := &responses.StrategyPackage{}
	err := json.Unmarshal(raw, pm)
	if err != nil {
		return nil, xerrors.Errorf("cannot convert to StrategyPackage. unmarshal error: %w", err)
	}
	if len(pm.Value) == 0 {
		return nil, xerrors.New("Result data is not exist")
	}
	if len(pm.Value) > 10 {
		l.Info("raw data has too many Results. %d Results exist. show the first 10 of Results array", len(pm.Value))
	}

	strategyPackage := make([]StrategyPackage, 0, 10)
	for i := 0; i < 10 && i < len(pm.Value); i++ {
		data := pm.Value[i]
		strategyPackage = append(strategyPackage, StrategyPackage{
	TaskListType:                   data.TaskListType,
	TaskListGroup:                  data.TaskListGroup,
	TaskListGroupCounter:           data.TaskListGroupCounter,
	TaskListSequence:               data.TaskListSequence,
	TaskListOperationInternalID:    data.TaskListOperationInternalID,
	MaintenancePackage:             data.MaintenancePackage,
	MaintPckgTskListOpAllocIntNmbr: data.MaintPckgTskListOpAllocIntNmbr,
	MaintenancePackageText:         data.MaintenancePackageText,
	MaintenanceTaskListOperation:   data.MaintenanceTaskListOperation,
	OperationText:                  data.OperationText,
	MaintTaskListSubOperation:      data.MaintTaskListSubOperation,
	ChangeNumber:                   data.ChangeNumber,
		})
	}

	return strategyPackage, nil
}

func ConvertToOperation(raw []byte, l *logger.Logger) ([]Operation, error) {
	pm := &responses.Operation{}
	err := json.Unmarshal(raw, pm)
	if err != nil {
		return nil, xerrors.Errorf("cannot convert to Operation. unmarshal error: %w", err)
	}
	if len(pm.Value) == 0 {
		return nil, xerrors.New("Result data is not exist")
	}
	if len(pm.Value) > 10 {
		l.Info("raw data has too many Results. %d Results exist. show the first 10 of Results array", len(pm.Value))
	}

	operation := make([]Operation, 0, 10)
	for i := 0; i < 10 && i < len(pm.Value); i++ {
		data := pm.Value[i]
		operation = append(operation, Operation{
	TaskListType:                   data.TaskListType,
	TaskListGroup:                  data.TaskListGroup,
	TaskListGroupCounter:           data.TaskListGroupCounter,
	TaskListSequence:               data.TaskListSequence,
	TaskListOperationInternalID:    data.TaskListOperationInternalID,
	TaskListOpBOMItmIntVersCounter: data.TaskListOpBOMItmIntVersCounter,
	MaintenanceTaskListOperation:   data.MaintenanceTaskListOperation,
	MaintOperationExecStageCode:    data.MaintOperationExecStageCode,
	OperationText:                  data.OperationText,
	OperationControlProfile:        data.OperationControlProfile,
	WorkCenter:                     data.WorkCenter,
	Plant:                          data.Plant,
	Assembly:                       data.Assembly,
	OperationCalculationControl:    data.OperationCalculationControl,
	OpPlannedWorkQuantity:          data.OpPlannedWorkQuantity,
	OpWorkQuantityUnit:             data.OpWorkQuantityUnit,
	NumberOfCapacities:             data.NumberOfCapacities,
	PurchaseOrderQty:               data.PurchaseOrderQty,
	PurchaseOrderQuantityUnit:      data.PurchaseOrderQuantityUnit,
	OperationStandardDuration:      data.OperationStandardDuration,
	OperationStandardDurationUnit:  data.OperationStandardDurationUnit,
	CostCtrActivityType:            data.CostCtrActivityType,
	MaterialGroup:                  data.MaterialGroup,
	OpExternalProcessingPrice:      data.OpExternalProcessingPrice,
	OpExternalProcessingCurrency:   data.OpExternalProcessingCurrency,
	CostElement:                    data.CostElement,
	PurchasingGroup:                data.PurchasingGroup,
	PurchasingOrganization:         data.PurchasingOrganization,
	PurchaseContract:               data.PurchaseContract,
	PurchaseContractItem:           data.PurchaseContractItem,
	Supplier:                       data.Supplier,
	ChangeNumber:                   data.ChangeNumber,
	PurchasingInfoRecord:           data.PurchasingInfoRecord,
	IsBusinessPurposeCompleted:     data.IsBusinessPurposeCompleted,
	SupplierAccountGroup:           data.SupplierAccountGroup,
	TaskListStatus:                 data.TaskListStatus,
	ResponsiblePlannerGroup:        data.ResponsiblePlannerGroup,
	MaintenancePlanningPlant:       data.MaintenancePlanningPlant,
	MaintenancePlannerGroup:        data.MaintenancePlannerGroup,
	ControllingArea:                data.ControllingArea,
	CostCenter:                     data.CostCenter,
	MaintenancePlant:               data.MaintenancePlant,
	ValidityStartDate:              data.ValidityStartDate,
	ValidityEndDate:                data.ValidityEndDate,
	TechnicalObject:                data.TechnicalObject,
	TechObjIsEquipOrFuncnlLoc:      data.TechObjIsEquipOrFuncnlLoc,
		})
	}

	return operation, nil
}

func ConvertToOperationMaterial(raw []byte, l *logger.Logger) ([]OperationMaterial, error) {
	pm := &responses.OperationMaterial{}
	err := json.Unmarshal(raw, pm)
	if err != nil {
		return nil, xerrors.Errorf("cannot convert to OperationMaterial. unmarshal error: %w", err)
	}
	if len(pm.Value) == 0 {
		return nil, xerrors.New("Result data is not exist")
	}
	if len(pm.Value) > 10 {
		l.Info("raw data has too many Results. %d Results exist. show the first 10 of Results array", len(pm.Value))
	}

	operationMaterial := make([]OperationMaterial, 0, 10)
	for i := 0; i < 10 && i < len(pm.Value); i++ {
		data := pm.Value[i]
		operationMaterial = append(operationMaterial, OperationMaterial{
	TaskListType:                   data.TaskListType,
	TaskListGroup:                  data.TaskListGroup,
	TaskListGroupCounter:           data.TaskListGroupCounter,
	TaskListSequence:               data.TaskListSequence,
	TaskListOperationInternalID:    data.TaskListOperationInternalID,
	TaskListOpBOMItmInternalID:     data.TaskListOpBOMItmInternalID,
	TaskListOpBOMItmIntVersCounter: data.TaskListOpBOMItmIntVersCounter,
	MaintenanceTaskListOperation:   data.MaintenanceTaskListOperation,
	Material:                       data.Material,
	MaterialName:                   data.MaterialName,
	BillOfMaterialItemQuantity:     data.BillOfMaterialItemQuantity,
	BillOfMaterialItemCategory:     data.BillOfMaterialItemCategory,
	BillOfMaterialItemUnit:         data.BillOfMaterialItemUnit,
	ResvnIsMRPRlvtOrPurReqnIsCrted: data.ResvnIsMRPRlvtOrPurReqnIsCrted,
	MatlCompIsMarkedForBackflush:   data.MatlCompIsMarkedForBackflush,
	SafetyRelevantObject:           data.SafetyRelevantObject,
	BillOfMaterialItemCategoryDesc: data.BillOfMaterialItemCategoryDesc,
	MatlsPlngRelevancyCodeName:     data.MatlsPlngRelevancyCodeName,
	SafetyRelevanceActionDesc:      data.SafetyRelevanceActionDesc,
		})
	}

	return operationMaterial, nil
}
