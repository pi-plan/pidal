## 切换顺序一定是
0. ACTIVE
1. BLOCK
2. RESHARDING
3. ACTIVE

## Zone Sharding 切换
0. Frontend 服务启动切换。
1. 服务状态进入到 UPDATEING，标记为切换中，并通知所有的 session 开始更新，并计数。
2. 判断 Session 状态，如果是在事务中，就继续沿用当前版本使用。等事务完成后切换到新版本。因为事务超时时间是 5S，所以最长也就是 5 秒就全部切换到了新版本上。返回切换成功。
3. 新创建的 session 会直接使用新版本。
4. 如果中间 session 异常中断，在关闭的时候判断是否在 UPDATING 中，并完成计数。
5. 计数清零的时候好酥 pimms 已经完成。

## DB 切换

0. 新老连接池都保留直到被关闭才可以删除。
1. 如果是相同的就复用。
2. 新老配置引用不同的 poll
