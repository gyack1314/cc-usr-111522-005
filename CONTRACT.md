# 数据与接口约定

目录资源路径为(database,table,partition)，前缀是父资源，不允许跨数据库混作祖先。事务状态active/rolling_back/committed/aborted。现有事务stage将完整资源值放入私有区，commit一次发布，rollback分阶段清理私有写入后才转aborted。锁模式IS、IX、S、SIX、X按标准多粒度兼容表：IS兼容IS/IX/S/SIX，IX兼容IS/IX，S兼容IS/S，SIX仅兼容IS，X不兼容其他事务。子S需要祖先IS或更强意向，子X需要祖先IX/SIX/X。事务自己的锁可重入，request_id同一请求幂等，显式多次不同request可产生多个持有句柄；升级保留原锁直到成功或事务完成。新请求不能越过已经等待的冲突写请求；取消等待不撤销已经持有的保护。重启恢复由现有持久提交层承担，本模块关注进程内并发目录操作。
