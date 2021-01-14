- 一个 server 就是一个逻辑上的数据库，不管后端实际上是多少个数据库，一个 server 只会是一个。
- database, table  都是逻辑上的。
 
frontend 负责认证，和结果输出。
认证后连接 db 的session 。
db 输出 result ，frontend 负责转换协议。
