-- 派工验收夹具重置: 清理上一轮运行痕迹后重建(dispatch_fixture.sql 的幂等包装)
DELETE r FROM liaoyun.mes_work_rec r JOIN liaoyun.mes_task t ON r.task_id = t.id
 WHERE t.work_id = 9007 OR t.sn IN ('SN-DISPATCH-01','LOAD-FILLER-1','LOAD-FILLER-2');
DELETE FROM liaoyun.mes_task WHERE work_id = 9007 OR sn IN ('SN-DISPATCH-01','LOAD-FILLER-1','LOAD-FILLER-2');
DELETE FROM liaoyun.mes_ycrec WHERE sn = 'SN-DISPATCH-01';
DELETE FROM liaoyun.mes_workplace_suspend;
DELETE FROM liaoyun.mes_work WHERE id = 9007;
DELETE FROM liaoyun.mes_workplaces WHERE id IN (9201,9202,9203,9204);
DELETE FROM liaoyun.mes_prosetting WHERE id IN (6101,6102);
DELETE FROM liaoyun.mes_product WHERE id = 8006;
DELETE FROM liaoyun.mes_process WHERE id IN (3001,3002);

INSERT INTO liaoyun.mes_process (id, title, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES (3001, 'Dispatch-Op1', '0', '0', '000000', 103, 1, NOW(), 1, NOW()), (3002, 'Dispatch-Op2', '0', '0', '000000', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_product (id, title, sn, stocks, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES (8006, 'Dispatch-Demo-Product', 'P-DISP-01', 7001, '0', '0', '000000', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_prosetting (id, pro_id, process_id, sort, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES (6101, 8006, '3001', 1, '0', '0', '000000', 103, 1, NOW(), 1, NOW()), (6102, 8006, '3002', 2, '0', '0', '000000', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_workplaces (id, sn, title, process_ids, counts, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES
 (9201, 'WP-1', 'Station-Op1-LowLoad', '3001', 1, '0', '0', '000000', 103, 1, NOW(), 1, NOW()),
 (9202, 'WP-2', 'Station-Op1-HighLoad', '3001', 1, '0', '0', '000000', 103, 1, NOW(), 1, NOW()),
 (9203, 'WP-3', 'Station-Op2-AlarmDev', '3002', 1, '0', '0', '000000', 103, 1, NOW(), 1, NOW()),
 (9204, 'WP-4', 'Station-Op2-Backup', '3002', 1, '0', '0', '000000', 103, 1, NOW(), 1, NOW());
UPDATE liaoyun.mes_workplaces SET equ_ids = 'DEV-ALARM-01' WHERE id = 9203;
INSERT INTO liaoyun.mes_task (id, sn, status, workplace_id, needcount, markcount, work_id, pro_id, currproid, currprocess, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES
 (9211, 'LOAD-FILLER-1', '1', 9202, 10, 0, 9006, 8006, 3001, 1, '000000', '0', 103, 1, NOW(), 1, NOW()),
 (9212, 'LOAD-FILLER-2', '1', 9202, 10, 0, 9006, 8006, 3001, 1, '000000', '0', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_work (id, sn, pro_id, title, status, okprocess, processcount, currprocess, neednum, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES (9007, 'SN-DISPATCH-01', 8006, 'Dispatch-Demo-Work', '1', 0, 2, '', 3, '000000', '0', 103, 1, NOW(), 1, NOW());
