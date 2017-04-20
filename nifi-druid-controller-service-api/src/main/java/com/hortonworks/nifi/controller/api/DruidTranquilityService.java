package com.hortonworks.nifi.controller.api;

import java.util.Map;

import org.apache.nifi.controller.ControllerService;

import com.metamx.tranquility.tranquilizer.Tranquilizer;

public interface DruidTranquilityService extends ControllerService{
    Tranquilizer<Map<String,Object>> getTranquilizer();
}