# PiDAL

PiDAL(Pi Data Access Layer) 是一个纯异步、高性能、兼用 MySQL 通讯协议的数据库中间件。不仅提供了常规的分库、分表、读写分离等功能，还基于 [A2PC 协议](/a2pc/introduction) 实现了分布式事务处理能力，无入侵、对客户端完全透明，无需额外兼容即可保证分库分表后数据的正确性。

- [文档](https://plan.3.1415926.me/#/pidal/introduction)


PiDAL 不仅支持以 DB Proxy 模式部署，在容器化实施的比较完善的场景下，PiDAL 也能以 Sidecar 的模式部署，可以降低不必要的性能消耗。DB Proxy 和 Sidecar 模式之间的区别和优缺点对比详情可以 [点击这里查看](/pidal/introduction?id=driver、sidecar、dbproxy)。

另外，PiDAL 还对分布式数据库中常见的问题，PiDAL 提供了 [双 Sharding 表](/pidal/sharding-paging) 解决方案，在开发过程中减少因为分库对开发过程的影响。在 PiDTS 组件的协同下，不仅支持数据库在线扩容也支持数据库 Sharding 规则的重新调整。
PiDAL 也支持在先热更新，不需要重启，即可平滑的升级配置规则，在配置更新过程中，会平滑的维护好每个会话和事务，保证平滑结束，对客户端完全透明。
