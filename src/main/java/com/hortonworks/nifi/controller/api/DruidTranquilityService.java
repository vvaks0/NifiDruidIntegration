package com.hortonworks.nifi.controller.api;

import org.apache.nifi.controller.ControllerService;

import com.metamx.tranquility.tranquilizer.Tranquilizer;

public interface DruidTranquilityService extends ControllerService{
    Tranquilizer getTranquilizer();
}