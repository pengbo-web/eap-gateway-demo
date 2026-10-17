-- OEE 夹具重置: 清理旧日期行后按当天 CURDATE 重建
DELETE FROM liaoyun.mes_eap_device_spec WHERE id = 5001;
DELETE FROM liaoyun.mes_eap_device_state_log WHERE id IN (6001,6002,6003);
DELETE FROM liaoyun.mes_work_rec WHERE id IN (7001,7002);

INSERT INTO liaoyun.mes_eap_device_spec (id, device_code, ideal_cycle_sec, planned_minutes, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES (5001, 'DEV-OEE-TEST', 6, 1440, '000000', '0', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_eap_device_state_log (id, device_code, from_status, to_status, event_time, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES
 (6001, 'DEV-OEE-TEST', NULL, '0', CONCAT(CURDATE(),' 08:00:00'), '000000', '0', 103, 1, NOW(), 1, NOW()),
 (6002, 'DEV-OEE-TEST', '0', '1', CONCAT(CURDATE(),' 09:00:00'), '000000', '0', 103, 1, NOW(), 1, NOW()),
 (6003, 'DEV-OEE-TEST', '1', '0', CONCAT(CURDATE(),' 09:30:00'), '000000', '0', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_work_rec (id, task_id, count, defective, status, device_code, params, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES
 (7001, 9103, 100, 4, '2', 'DEV-OEE-TEST', '{"v":220}', '000000', '0', 103, 1, CONCAT(CURDATE(),' 09:00:00'), 1, CONCAT(CURDATE(),' 09:00:00')),
 (7002, 9103, 50, 2, '2', 'DEV-OEE-TEST', '{"v":230}', '000000', '0', 103, 1, CONCAT(CURDATE(),' 09:20:00'), 1, CONCAT(CURDATE(),' 09:20:00'));
