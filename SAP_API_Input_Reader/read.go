package sap_api_input_reader

import (
	"encoding/json"
	"fmt"
	"os"
)

func ConvertToECMC(data map[string]interface{}) EC_MC {
	raw, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("data marshal error :%#v", err.Error())
		return EC_MC{}
	}
	ec := EC_MC{}
	err = json.Unmarshal(raw, &ec)
	if err != nil {
		fmt.Printf("input data marshal error :%#v", err.Error())
		os.Exit(1)
	}

	return ec
}

func ConvertToSDC(data map[string]interface{}) SDC {
	raw, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("data marshal error :%#v", err.Error())
		return SDC{}
	}
	sdc := SDC{}
	err = json.Unmarshal(raw, &sdc)
	if err != nil {
		fmt.Printf("input data marshal error :%#v", err.Error())
		os.Exit(1)
	}

	return sdc
}
