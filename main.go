package main

import (
	sap_api_caller "sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Caller"
	sap_api_input_reader "sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Input_Reader"
	"sap-api-integrations-maintenance-task-list-reads-rmq-kube/config"

	"github.com/latonaio/golang-logging-library/logger"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client"
	"golang.org/x/xerrors"
)

func main() {
	l := logger.NewLogger()
	conf := config.NewConf()
	rmq, err := rabbitmq.NewRabbitmqClient(conf.RMQ.URL(), conf.RMQ.QueueFrom(), conf.RMQ.QueueTo())
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Close()

	caller := sap_api_caller.NewSAPAPICaller(
		conf.SAP.BaseURL(),
		conf.RMQ.QueueTo(),
		rmq,
		l,
	)

	iter, err := rmq.Iterator()
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Stop()

	for msg := range iter {
		err = callProcess(caller, msg)
		if err != nil {
			msg.Fail()
			l.Error(err)
			continue
		}
		msg.Success()
	}
}

func callProcess(caller *sap_api_caller.SAPAPICaller, msg rabbitmq.RabbitmqMessage) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = xerrors.Errorf("error occurred: %w", e)
			return
		}
	}()
	taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter, equipment, plant, taskListSequence, maintenancePackageText, technicalObject, operationText := extractData(msg.Data())
	accepter := getAccepter(msg.Data())
	caller.AsyncGetMaintenanceTaskList(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter, equipment, plant, taskListSequence, maintenancePackageText, technicalObject, operationText, accepter)
	return nil
}

func extractData(data map[string]interface{}) (taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter, equipment, plant, taskListSequence, maintenancePackageText, technicalObject, operationText string) {
	sdc := sap_api_input_reader.ConvertToSDC(data)
	taskListType = sdc.MaintenanceTaskList.TaskListType
	taskListGroup = sdc.MaintenanceTaskList.TaskListGroup
	taskListGroupCounter = sdc.MaintenanceTaskList.TaskListGroupCounter
	taskListVersionCounter = sdc.MaintenanceTaskList.TaskListVersionCounter
	equipment = sdc.MaintenanceTaskList.Equipment
	plant = sdc.MaintenanceTaskList.Plant
	taskListSequence = sdc.MaintenanceTaskList.StrategyPackage.TaskListSequence
	maintenancePackageText = sdc.MaintenanceTaskList.StrategyPackage.MaintenancePackageText
	technicalObject = sdc.MaintenanceTaskList.StrategyPackage.Operation.TechnicalObject
	operationText = sdc.MaintenanceTaskList.StrategyPackage.Operation.OperationText
	return
}

func getAccepter(data map[string]interface{}) []string {
	sdc := sap_api_input_reader.ConvertToSDC(data)
	accepter := sdc.Accepter
	if len(sdc.Accepter) == 0 {
		accepter = []string{"All"}
	}

	if accepter[0] == "All" {
		accepter = []string{
			"Header", "HeaderEquipmentPlant", "StrategyPackage", "StrategyPackageText",
			"Operation", "OperationText", "OperationMaterial",
		}
	}
	return accepter
}
