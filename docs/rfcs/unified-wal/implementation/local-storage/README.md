# 隔离本地存储实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE。

施工清单仍由[上层 README](../README.md)拥有，本目录只记录步骤1开始的本地存储实现与验证。源码在现有 `bookkeeper-server` 模块的 `org.apache.bookkeeper.bookie.storage.segment` 包，不接入生产 Bookie、LedgerStorage、Journal、Add 或 ACK。

## 当前实现

步骤1：`IsolatedSegmentConfig` 固定单 Arena、4个逻辑 shard、两个16 MiB DATA文件、4096字节对齐、1 MiB payload上限、256个pending上限及 FILE_CHANNEL/OFF/SYSTEM/PARK。参数检查拒绝容量溢出、非对齐容量和未实现模式。配置合同由步骤3的文件实现消费。

执行位置为 `/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper` 的 `turbo-bk`；规划输入提交为 `e75c67f3514d1627ea790d052e2c9226e593aa3c`，代码原基线为 `e8b162076fd8cfa84a11351a82caac8c5ac1f939`。完整执行目标见[执行正文](../execution-objective.md)。

步骤3：已实现[实验格式v1](format-v1.md)、DATA/控制framing writer与独立parser、固定字节向量、全容量预创建文件及必要目录同步。真实FileChannel处理短写后force，失败保留坐标并停止受影响文件写入；容量满额停止。文件创建时控制文件只有身份块；framing parser本身不授予allocator权限。逐entry成功、selector和restart服务仍未实现。

步骤4的第一部分：`SegmentOwnerLoop` 用固定平台线程承载静态 shard 子集，ledger→shard 只依赖固定逻辑 shard 数，worker 数不进入持久身份。DATA与控制任务分别限额，异步任务预留各自的completion槽；控制任务在DATA准入额度满时仍可提交阻塞I/O，结果回原owner。循环每轮处理控制和DATA完成、控制任务及各shard的一个输入。PARK遵守最老batch deadline，后来的deadline不能推迟它。`SegmentIoExecutor`在启动线程按同一I/O线程总额预创建DATA与控制两个有界服务，隔离group要求至少2个I/O线程，其中1个及独立有界队列留给控制；DATA工作线程/队列满时不会拒绝已取得控制额度的控制I/O。提交后handler异常、停止准入或等待超时不提前释放在途义务。控制I/O服务永久故障或控制自身额度满时仍需背压/隔离。

该循环尚未接入实际 append/allocator 状态机，现已通过下述group启动接入绑定与SYSTEM/OWNER_TOUCH对齐池。容量由构造参数显式给定并从同一总量拆分，不能给每个owner复制全局额度。

步骤4的显式绑定API：现有 `cpu-affinity` 模块新增 `CpuBinding`，只作用于调用平台线程，不调用旧 `acquireCore`，不改 root/CPU online/sibling/文件锁策略。动态 JNI mask 支持稀疏编号（上限65536），设置后读取实际mask核对；绑定失败回滚，关闭只允许原线程执行，恢复不完整明确报错。拒绝空mask、越界、超出初始mask和虚拟线程；提供运行期mask复查。该API已接入隔离group启动；配置接受OFF/PREFER/REQUIRE启动模式，默认OFF。`closeTo`可在原线程将PREFER绑定释放到当前合法的初始mask子集，设置和读回不一致则明确失败。

步骤4的拓扑与静态placement：`CpuTopologySnapshot` 在缩窄应用线程mask前只读捕获初始mask、online CPU、cgroup v2有效CPU/memory-node集合及可见祖先最小quota，读取package/core/SMT sibling/NUMA信息。不跨socket合并同号core，不一致/缺失信息标未知；隐藏祖先明确不可观测。DATA设备节点只消费调用者已确认的sysfs `numa_node` 路径，未提供或-1保持UNKNOWN，不自动猜测复合设备归属。

`ExecutionPlacementPlan` 按固定逻辑shard映射worker，每个append worker选一个不同物理核的CPU，并排除其整个SMT sibling组给服务线程留余量。显式配置冲突拒绝；quota未知/不足、核不足或物理拓扑未知不生成绑定计划。内存节点不在允许集合时不给pool节点偏好；该计划不设置NUMA策略，也不证明物理页属地。OFF映射不要求native拓扑，CPU/worker变化不改变逻辑shard数。该计划已接入以下group启动；运行期重检查仍是步骤4剩余工作。

步骤4的group启动与池：`SegmentExecutionGroup` 从未缩窄mask的启动线程创建固定owner/I/O线程，在线程入口绑定、分配池并报告ready，全部ready后才开放准入。初始化失败先停止准入、解除启动等待，再join所有已启动线程；pool和binding都在其原线程关闭。PREFER对拓扑/native不可用可退回OFF；绑定阶段失败只有在已验证清理成功且无其他初始化错误时才能重建OFF组，清理不确定不伪报降级成功。REQUIRE启动失败不返回可用group。

`SegmentBufferPool` 用固定数量Netty unpooled direct backing buffer提供4096对齐view，支持SYSTEM和OWNER_TOUCH，均在owner上初始化；OWNER_TOUCH不宣称页必然本地。全局pending/control/buffer数量按owner分摊，direct容量预算包含每块alignment padding；这是请求容量上限，不是整个进程RSS。每个物理buffer预留归还槽，跨线程归还回原池并唤醒owner，不占DATA/control槽。lease携带原池实例、owner和generation，重复归还被拒绝。关闭等待已准入命令和所有live lease；超时返回未终结状态，不释放在途资源。owner排空后才停止I/O服务。

步骤4的运行期检查：绑定模式额外启动一个服务mask上的低频监测线程，每秒以启动时保存的部署mask重读online CPU、cgroup有效CPU/memory-node和可见quota，不把单核owner当前mask当成部署额度。owner在循环中每秒复查自身实际mask，I/O服务线程在任务前限频复查；检查不在每次Add读取sysfs。PREFER遇到计划mask/quota失效时保留原shard/owner和已借buffer，原线程释放绑定到当前合法CPU集合并记录原因；空合法集合或释放失败停止新准入。空闲I/O线程在下一项任务前释放绑定；若服务线程替换时仍继承旧窄mask，拒绝把它当成成功降级并停止准入。REQUIRE发现计划或实际mask失效时停止新准入，已受理命令和buffer仍经原owner完成/归还；若owner低频placement check直接抛出异常，也由原owner记录失败、停收并排空已提交I/O与排队ticket，避免线程直接退出遗留义务。调用方须执行现有drain/shutdown并决定重启。memory-node变化只记录已有池属地可能下降，不迁移或提前释放buffer。当前没有接入Bookie生命周期的自动重启，因此完整§10.5服务退出编排仍未完成。

PREFERRED专有mapping、实际DATA批写接入及BOUNDED_SPIN收益对照也尚未完成；SYSTEM/OWNER_TOUCH/PARK保持默认，当前仍是隔离可丢弃实验实现。

步骤5的Arena局部切片：`SegmentArenaControlLog` 在已创建的隔离目录中验证所有必需文件身份，用独占文件锁打开`arena-control`，对`ALLOC`/`ALLOC_POOL`建立有界、仅追加的控制历史。调用方提交的batch先复制为不可变命令列表，完整写入后一次`force(true)`；只有随后按sequence逐条apply成功才产生空间授权。同批争用slot的后续命令为消耗sequence的确定性no-op。操作UUID和命令内容用于重复/不兼容解析；range绑定file、offset、allocation ID/generation、owner shard及其generation和该stream首个物理sequence；同一allocation ID不重复授予另一范围，同一shard/generation的后续分配不能改写首序号。重放后的每个DATA文件可查询已成功授权block的最高结束位置；未apply或条件失败命令不推进这一水位，全零的已授权block仍计入。它只是保守授权范围事实，不证明DATA used/unused或可写起点。`SegmentArenaControlDriver`一次只受理一个batch，在控制专用I/O服务执行`appendDurably`，再由原owner的独立控制completion执行`applyDurable`并返回结果；DATA准入和DATA I/O服务满额、停收准入及重开重放均不跳过已受理的控制义务。重开先验证完整记录/连续序号与身份，再同步并重放；Arena对已追加的Bookie控制文件只核对至少4096字节的身份头，不能要求其仍为初建长度。不确定I/O使当前实例停止授予权限，无法安全分类的尾部拒绝可写恢复，不猜测截断。日志满额停止接收，隔离驱动尚未接入Bookie生命周期，checkpoint/rotation也未实现。

步骤5的Bookie局部控制切片：`ProtectedProfileCredential`用外部提供的AES密钥及存储scope作为AAD，将20字节verifier封成定长AES-GCM字段；重启解封后仍由原包装在内存中作常量时间比较，普通Add不需要逐请求HMAC。错误密钥、scope或字段篡改均拒绝解封，包装自身仍不暴露明文byte accessor。`SegmentBookieControlLog`在独立`bookie-control`文件上验证存储身份并取得独占锁，把INSTALL的route/instance/descriptor/credential与initial inactive放在一条记录，ACTIVATE在另一条记录。它对有界batch先完整写入并一次force，再按sequence条件apply；同一操作重试、同一语义的新操作、冲突凭据可在重开后确定性解析。完整记录重开前先同步，损坏/不完整尾部与错误密钥拒绝可写打开。局部normal准入只有匹配context和verifier且ACTIVATE已apply才放行；此查询不代表DATA持久化或成功确认。FENCE受控入口先关闭新准入，已准入DATA票据需保持到物理I/O终结，终结前只返回等待；之后才将FENCE写入并force，重开可恢复fenced拒绝。受控`TOMBSTONE_OR_DELETE_APPLY`以typed committed authority写入同一日志，匹配实例先关闭normal新准入及在途结果资格，force后建立不可逆terminal route；无需等待旧DATA物理终结，同批其他ledger仍可取得本地成功，重放后迟到INSTALL/ACTIVATE不能复活该ledger。错误实例的tombstone不切断当前route；已force的DATA物理候选仍保留，但不因此恢复被删除实例的成功资格。预先取得的逐record票据已接入隔离DATA物理驱动，直到真实I/O与原owner回调均终结才释放；Bookie控制owner驱动让INSTALL/ACTIVATE与FENCE经独立控制I/O force，再由原owner apply；FENCE等待pre-cut票据时只返回可重试状态，不占住owner。受控explicit LAC经exact instance/credential与normal-active检查后，在独立控制记录中保存单调数值及不透明body；write/force成功才apply并返回，同值同body可在fence后解析为已持久，较低值和同值不同body均不改写。重开先同步并重放完整LAC记录，force结果不确定时原调用不得成功；pending fence等待先开始的LAC barrier。该切片现可由`SegmentBookieControlDriver`在原owner预留，在控制I/O服务write/force，completion回原owner apply；未开始I/O就被执行器拒绝时撤销预留，可用同一sequence重试，慢force不占住owner。仍逐条force，未接控制group commit、Profile网络读写或客户端confirmed范围。当前没有密钥供应/轮换、真实MetadataStore和mTLS授权接线；完整recovery请求生命周期、完整read权限/selector、delete-applied cursor、清理义务与物理回收也未实现。密钥必须独立于隔离数据目录保存，缺失时不得开放该scope。

Arena控制日志在短状态锁内预留序号与物理后缀，释放锁后执行文件write/force；已apply的grant查询在force等待期间仍可完成，关闭则拒绝在途I/O，直到结果终结后才允许关闭。`IsolatedSegmentFiles`也只在短状态锁内预留DATA范围、登记在途I/O和失败状态；同文件write/force在独立文件级锁中串行，另一个预留在慢force期间仍能推进，close拒绝关闭活跃I/O。DATA owner尚未接入完整请求级生命周期；隔离逐entry结果不能据此宣称普通Add非阻塞闭环。

隔离 `RECOVERY_GRANT` 使用实验性SHA-256 public payload identity绑定RepairIntent ID/generation、grant generation、目标和exact range；记录force后apply并重放，局部 `ADD_RECOVERY` 准入验证credential和完整tuple，tombstone撤销grant。grant现由原owner提交到预留控制I/O服务write/force，completion回owner apply，期间不提前开放准入。受控close在原grant的在途票据终结前保持WAITING，并立刻切断新准入及旧票据结果资格；票据终结后同样经预留控制I/O服务force、回owner apply，才发布局部committed-readable控制事实，重放后仍关闭该grant。执行前拒绝I/O时只撤销日志预留、保留准入cut，可不跳sequence重试；force失败保持本进程fail-closed，不伪造成功。重叠grant任一仍活跃时不报告该entry的局部可读事实。recovery票据现绑定wire entry bytes，经同一物理DATA force、连续前缀和热定位发布；受控同坐标重试通过真实点读比较逻辑payload，相同返回durable、冲突拒绝。慢force与close并发时，旧写完成但其局部结果为stale，close等物理与owner终结；重开扫描可找到完整DATA候选，但裸候选不证明当前有效位置和权限。真实RepairIntent冷读/mTLS授权、完整网络请求生命周期和实际读可见性仍未接线。

只读重开后的受控recovery物理候选比对现限定于尚无MOVE/FREE的单Arena：从已apply Arena状态确定目标shard唯一stream及持久首序号，扫描连续物理前缀并同步DATA文件；目标坐标必须只有一个普通DATA定位。持有重放Bookie授予的exact recovery票据，真实点读比较应用payload和累计ledger length，完成时复核grant及stream身份，才返回`MATCHED_CANDIDATE`或`CONFLICTING_CANDIDATE`。旧进程DATA I/O错误尚无持久分类，因此这两者都不构成durable duplicate结果或新写准入。同步失败不返回结果；多个stream generation、重复物理坐标或未定位返回`NOT_READY`，close在同步期间切断准入则返回`STALE_ADMISSION`。该入口不开放writer，不证明原请求ACK、完整current-selector/必要DATA选择、真实RepairIntent冷读、网络授权或两次掉电后的恢复。

真实子进程边界回归现让隔离recovery DATA经owner/I/O路径执行：一次在本地`LOCAL_DURABLE`结果后直接终止，另一次在完整写入后、DATA barrier前直接终止。父进程重新打开双控制日志、同步目标DATA文件并点读候选，两种情况下都只得到`MATCHED_CANDIDATE`物理比对。`Runtime.halt`不执行正常资源关闭，可验证进程锁释放与重放入口；它不是掉电模拟，不证明旧force失败分类、客户端ACK或可写restart。

`IsolatedSegmentFiles.writeAuthorized`已在实际FileChannel写入前核对同一目录/storage/Arena身份、冻结DATA batch的整段物理范围和上述grant身份；跨块batch要求同一pool授权，未apply或身份不符时不写入。它只约束这个显式调用路径；底层`write`保留给格式/I/O实验。`ALLOC_POOL`尚未实现DATA framing的used/unused恢复，也不支持FREE后的复用。Bookie局部INSTALL/ACTIVATE/FENCE/TOMBSTONE已与隔离物理DATA路径串联，但完整请求级语义、生产逐entry成功与完整启动恢复尚未接入，故步骤5未完成，不能据此开放Bookie写入。

只读`SegmentDataRecovery.inspect`要求DATA writer已退出并取得隔离目录锁，验证文件身份后逐页扫描；完整DATA batch必须通过独立parser、匹配的已apply Arena grant及每条record的固定ledger→logical shard映射，否则拒绝本次检查。统计的全零页只是字节观测，不能直接认定pool unused或可复用。`validateAndSync`沿同一验证路径，在每个已检查文件返回前执行真实`force(false)`；验证或同步失败就不返回结果。`recoverPhysicalInventory`还可在显式entry容量内，完成全目录验证与同步后返回普通`ENTRY_DATA`的物理位置，不返回`RELOCATION_COPY`；容量不足也不返回部分清单。上述诊断与全量清单扫描全部隔离DATA文件；单stream候选已按Arena授权块筛选物理范围，但尚未由Bookie/current-selector authority选出必要范围，也不推导连续stream前缀、可发布的恢复locator或开放写入。

步骤6的物理前缀局部切片：`IsolatedSegmentFiles`已将有界DATA坐标预留与已预留批次的实际write/force分开。预留在I/O前占用固定文件范围；失败不退回坐标，并停止该文件新写。DATA文件关闭现在同时拒绝正在执行的I/O和尚未终结的预留；I/O服务在执行前拒绝任务时，原owner终结预留计数但不回退坐标，物理前缀保持失败。`SegmentPhysicalPrefix`只在原shard owner上接收同一stream generation的递增batch sequence；I/O completion必须对应已预留batch且其真实force返回，才可推进连续物理前缀。较晚batch先durable仍等待前批；前批同步不确定时不释放后批，不凭后批完整字节报告本地成功。当前tracker的起点由隔离调用者显式提供，但提交时必须等于Arena已apply并可重放的stream首序号；它没有跨重启的durable-through证明，只返回运行期物理可发布候选。

步骤6继续接入有界物理热候选：`SegmentHotDirectory`在原owner上从独立parser解析的冻结batch中核对每条record的固定shard，再提取普通`ENTRY_DATA`位置，在占用DATA坐标前预留固定locator与pending容量，再把预留绑定到同一真实文件reservation。只有`SegmentPhysicalPrefix`释放连续、已force的reservation后，才将这些位置从pending移入可点读候选；同批`RELOCATION_COPY`不进入普通候选。容量满额、重复逻辑坐标在写入前明确拒绝；目录准入仍不处理合法LAC封装变化，也不把拒绝冒充同坐标幂等。运行期force发布与重启扫描恢复的物理候选在目录中分别标记；后者可供点读验证；尽管Arena现记录stream首序号，仍缺少恢复的完整durable前缀及selector证明，不能用于幂等durable成功。候选自身没有Bookie route/fence/tombstone/selector权限，也不等于local result；无RocksDB接管或淘汰时达到硬上限就背压新DATA。

`SegmentDataPhysicalDriver`已把上述局部原语接成隔离的真实文件路径：原owner先占热候选容量、核对Arena grant并预留DATA坐标，再将write/force交给有界I/O服务；完成事件回原owner，只有连续前缀释放后才发布候选并回调。物理失败会将所有尚未完成的回调报错，保留已占坐标，不释放后批候选。受控normal入口在提交前核对冻结batch每条ENTRY_DATA与Bookie预准入票据的context及ledger的固定shard，失败或owner拒收时释放票据；已提交批次直到实际I/O与原owner回调均终结才释放，较早batch失败时不会因后批提前收到失败回调而越过fence切点。原物理实验入口仍只产出候选；normal入口在候选发布后、票据释放前逐entry检查捕获的准入generation，同批可分别返回`LOCAL_DURABLE`或`STALE_ADMISSION`，物理失败返回`UNKNOWN`且不产生成功。pending fence在已准入ticket之后，仍等待该ticket终结；单条逻辑失败不删除已force的物理候选。此结果仅为隔离本地判定，尚无网络deadline/断连接线、完整请求生命周期、完整read selector或Bookie ACK；跨重启的可发布前缀仍待重建。

同一冻结batch携带两个不同`ENTRY_DATA`时，隔离执行组路径以一次DATA force发布两个可点读物理候选；受控normal路径还验证两个ledger共享一次force但activation generation只让其中一条过期，另一条取得local result。同批两个有效pre-cut Admission在force暂停时，L1的pending fence拒绝新准入但等待已准入L1终结；L1/L2各自取得`LOCAL_DURABLE`，之后L1 fence才durable。提交后逐entry响应等待切片为每条record绑定独立`NormalRequest`；`stopWaiting()`原子撤销该请求的回调，不取消共享DATA、不释放Bookie票据或已占坐标。若回调已被领取则返回false，不承诺拦截已开始的回调。真实文件测试在force暂停时撤销L1响应等待：L1不同payload重试仍冲突，L2在force后得到`LOCAL_DURABLE`，L1相同payload重试命中已发布候选；I/O未终结时Bookie控制文件不能关闭。当前未实现显式终止pre-cut Add的路径，不能将activation/tombstone造成的逐entry失败称作fence失败；也未计量生产性能。

该驱动也已在`SegmentExecutionGroup`的现有owner/I/O服务上验证排空：已受理的真实DATA force挂起时，group关闭返回未终结且DATA文件拒绝关闭；force完成、原owner回调可点读候选后，group才排空退出。此为隔离执行组的局部生命周期证据，仍未接入Bookie服务启动与停止。

步骤6的重复路径：`SegmentEntryIdentity`复用独立parser的BK CRC32C、context和坐标检查；`SegmentDuplicateCheck`比较真实候选中同一ledger instance/entryId的应用payload bytes和累计ledger length。受控`SegmentDataPhysicalDriver.checkPublishedNormalDuplicate`在捕获Bookie准入后，先复制并校验至多1 MiB应用数据加36字节BK头的输入。原owner若发现该坐标已有normal DATA在途，相同逻辑内容持有受owner准入数量约束的等待票据，直到原批次连续force并发布候选后才复核Bookie准入返回`SAME_DURABLE`；不同内容在占位期返回`CONFLICT`，不发第二次物理写。原批次失败时等待者返回`UNKNOWN`，已提交的物理坐标仍受不可写状态保护。若已有运行期force发布的候选，则由现有有界I/O服务点读，completion回原owner复核准入generation及候选位置，才返回`SAME_DURABLE`或`CONFLICT`。合法LAC及相应CRC差异不造成冲突，payload或累计length变化冲突，坏输入CRC在提交前拒绝；读失败返回`UNKNOWN`，未定位或仅由重启扫描恢复的候选返回`NOT_READY`，不推断absence。尚无跨入口统一bytes总账、网络deadline/断连接线与可写restart幂等；`SegmentHotDirectory.reserve`仍在物理写入前拒绝第二个同坐标DATA，因此本切片不构成对外完整幂等Add。

受控重复请求现在也可传入独立`DuplicateRequest`：提交后`stopWaiting()`只原子撤销该调用的响应回调，不撤销原DATA的坐标或已启动的候选点读。等待原force的同内容重试继续占有有界owner ticket；已发布候选的慢点读继续持有Bookie票据和读取工作集，直到真实I/O与原owner完成。真实文件交错测试覆盖两条路径：前者停止等待后原force仍完成，后者停止等待期间fence保持`WAITING_PRECUT`，点读终结后才可持久化fence。网络deadline/断连接线与跨入口bytes总账仍未接通。

步骤7的隔离点读局部切片：`SegmentPointRead`从独立parser已验证的batch提取record位置；读取时核对当前Arena grant、ledger固定shard、DATA文件身份、batch header与locator长度，再以4096字节对齐的有界范围读取目标record并验证外层身份和内层BK CRC32C。读取上限按格式固定，不随新写入payload阈值降低。重开后可从上述物理清单重新取得位置并点读；`SegmentDataRecovery.recoverPhysicalStreamCandidate`在调用方给定shard、generation和预期起点后，先核对重放的Arena首序号，再仅对匹配shard与stream generation的batch按sequence排序并拒绝缺口、重复和容量超限；同文件其他stream的块只保留文件身份检查及文件级恢复同步，不解析其framing；目标stream的非零页仍须通过batch/record校验和完整grant核对，缺口、重复或损坏均拒绝。隔离真实文件回归证明旧stream的损坏不阻断另一stream物理候选，而全量清单仍拒绝损坏文件；共享文件的同步错误依然使候选失败。`SegmentHotDirectory.restorePhysical`可在原owner上把调用方选定、分别同步过的多个stream候选作为一组装入有界热目录；先核对每个候选的目录、持久文件配置及storage identity，再检查整组容量与重复坐标，全部通过后才发布。跨scope、容量不足、重复或仍有在途DATA时拒绝本次装入，不留下其中先通过的stream。单stream入口沿用这一路径；这不负责选择当前必要stream或授予selector/ACK权限。新写入阈值等非持久准入设置可以变化。该路径已有持久首序号核对，但不证明完整恢复前缀或当前Bookie selector；当前Arena不支持FREE/复用，故尚无selector复核与allocation pin合同。隔离测试已覆盖真实normal DATA force和durable fence后关闭、重开Bookie/Arena控制日志、重放fence并从同步后的单stream物理候选点读；这只是正常关闭后的只读重开，不是崩溃/掉电或完整读取授权证据。重开物理点读只验证字节和grant，不能据此推导原请求ACK或完整restart恢复。

隔离Bookie局部读取门槛现通过`ReadAdmission`绑定已安装的exact ledger instance与descriptor；它与normal写准入分离，inactive或fenced实例仍可取得读票据。`SegmentPointRead.readLocallyGated`在物理点读前取得票据，返回前复核Bookie route，最后释放；匹配实例的pending/durable tombstone使新读拒绝；若发生在旧票据的结果判定前，也使该结果失败，重放后仍拒绝，已force的物理字节仍可单独检查。关闭Bookie控制文件等待在途读票据。此入口没有接真实Profile read请求、网络读取权限或current-selector/pin，不把任意物理候选解释为合法对外读取。

只读`recoverLocallyReadableStreamCandidate`完整校验目标stream的batch及每条record，并在返回候选前完成文件恢复同步；仅将Bookie重放后的本地状态中仍有exact读route且未被pending/durable tombstone切断的普通DATA位置计入有界候选；旧的未筛选物理入口保持原语义。同批两个ledger中一方被tombstone时，另一方可用一个locator名额恢复并通过本地读门槛；被终结记录即使不返回定位，其损坏仍使目标batch恢复失败。该筛选不是完整current-selector、网络授权或可写restart判定，控制状态并发变化仍由后续读取门槛复核。

隔离文件层现可执行**仅用于物理追加**的重开：要求Arena控制状态没有未apply/I/O中的命令，先取得独占writer锁，在同一锁内完整验证、同步DATA并复核文件身份与大小，随后一直持锁至重开句柄关闭；每个文件的新指针设置在全部已apply授权块的最高结束位置。已授权块中的全零尾页、旧预留和失败坐标一律跳过，不按最后完整batch回退指针；后续仍须有新apply的Arena grant，重开句柄的原始无授权写入和会返回本地结果的DATA driver都拒绝。真实文件测试覆盖旧块零尾不复用、新stream在其后写入及双stream重开点读、活跃writer与恢复同步失败、未apply控制命令拒绝重开。此入口当前要求与Arena控制日志完全相同的隔离配置；非持久新写阈值的变更尚未接入该重开路径。该低层入口不解除旧DATA I/O错误，也不证明完整末端前缀、Bookie/current-selector或生产local success；完整服务级可写重启仍需要必要DATA选择、空间与旧I/O判定。

需要保留旧坐标时，`openAppendOnlyWithInventory`在上述同一次writer锁内对完整DATA执行有界物理清单扫描与恢复同步，并将扫描结果绑定到重开句柄。原owner可用`SegmentHotDirectory.restorePhysicalInventory`先验证scope、容量、重复坐标及无在途提交，再一次性装入普通DATA物理候选；旧坐标仅对经过此热目录的准入路径占位，候选仍标记为非local-durable。同次扫描在另一个显式stream容量内核对Arena持久首序号及已观测batch的物理顺序，`startupInventory().streamCandidate(...)`对连续stream返回受限物理候选，对缺口/重复拒绝，空stream返回首序号之前的空前缀。`SegmentPointRead.readRestartedSingleStreamLocallyGated`用重开句柄的有界读取pin保持同一writer锁，要求目标shard的Arena stream唯一、Bookie exact读route仍有效，并在真实record点读后复核两者；pin未结束时句柄拒绝关闭。它只读启动清单中的旧位置，不接管后续新DATA定位。任一容量不足或同步失败不返回句柄/部分清单，并释放扫描／重开持有的锁；热目录安装失败时，启动调用方仍须关闭已返回的句柄。DATA锁入口先在进程内登记同scope持有者，第二次获取在打开锁文件channel前被拒绝，以免失败关闭第二个channel释放原writer的POSIX锁。此路径尚未解析旧I/O错误、一般selector或Profile网络读授权，DATA driver继续拒绝在该重开句柄上返回local result。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`及`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest`：53项与29项通过，均0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。新增同句柄点读断言覆盖启动清单、读取pin阻止writer锁提前释放、fence后仍可读、tombstone后拒绝及同shard多stream无法唯一选择；这不是服务级restart或生产读取授权。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest`及`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：29项与53项通过，均0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。新增真实文件场景分别验证同次扫描的双stream前缀、无DATA的空前缀、第一批未写而后一批完整时拒绝连续候选，以及stream容量不足后可重新取得writer锁；已同步且物理连续不解除此前未知I/O结果或授予local-durable资格。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest#failedSameProcessRecoveryKeepsTheWriterLockAcrossProcesses`：修复前独立JVM在同进程重开失败后取得原writer锁，测试按预期失败；统一DATA锁入口后该测试通过。随后执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest,SegmentBookieControlLogTest`：81项通过，0失败/错误/跳过。该回归验证此macOS运行环境中“持锁→同进程重开与扫描拒绝→独立进程仍被排斥→原句柄关闭后可获取”；不证明所有文件系统的锁语义，也不完成服务级restart。

2026-09-29 分别执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest`和`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：27项与53项通过，均0失败/错误/跳过；`mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check -q`通过。新增文件级交接验证旧entry随受锁扫描进入启动清单、原owner占位、仍无local-durable资格；容量不足与同步失败后可重新取得writer锁。测试不证明完整服务启动、可写前缀、跨进程争锁时序或掉电保证。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest,SegmentBookieControlLogTest`：80项隔离测试通过，0失败/错误/跳过；`mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check -q`通过。现有重开测试覆盖活跃writer拒绝、同步失败后再次重开、物理追加与重启点读；本次改动使扫描、恢复同步与新writer打开共享同一次排他锁持有，不把这些测试扩展为跨进程竞争或掉电验收。

## 验证入口

```bash
scripts/unified-wal/run-local-storage-tests.sh IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentArenaControlLogTest,SegmentOwnerLoopTest,SegmentExecutionGroupTest
mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check
```

脚本为每次运行创建独立临时结果目录，保存HEAD/branch、源码及测试SHA-256、命令、平台、Maven输出、退出码和本次生成的JUnit XML。脚本不覆写历史receipt，不扫描或复制用户PDF。默认离线使用已缓存依赖；首次缺依赖时用 `UW_FETCH_DEPENDENCIES=true scripts/unified-wal/run-local-storage-tests.sh` 联网获取，后续不重复请求snapshot元数据。它通过 `-P '!cargo-zigbuild'` 禁用本实验不使用的Linux Rust交叉编译profile；该入口构建CPU affinity模块；Linux native绑定和拓扑另有下述容器验证，物理NUMA仍未验证。

首次探索性离线构建缺少datasketches/Vert.x依赖；未禁用上述profile的尝试因本机缺少cargo-zigbuild失败。这些是构建失败，不是测试PASS或故障注入结果。

## 当前验证

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：54项隔离测试通过，0失败/错误/跳过；`mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check -q`通过。同批双ledger共享真实DATA force时，L1的pending fence拒绝新准入，但两条pre-cut Add都取得`LOCAL_DURABLE`；原owner完成屏障后L1 fence才durable。该验证不覆盖显式终止pre-cut分支、生产ACK或崩溃恢复。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：48项隔离真实文件测试通过，0失败/错误/跳过。新增回归连续启动两个Java子进程：首个在recovery DATA本地结果后`Runtime.halt`，第二个重开Arena并同步DATA，在全部旧授权块之后取得新grant、写入另一个stream后再次`Runtime.halt`；父进程重演控制日志、同步并点读两个stream，旧recovery重复请求因同shard多stream仍为`NOT_READY`。该验证不模拟掉电，也不授予服务级可写重启、旧I/O错误解析或生产ACK。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentArenaControlLogTest,SegmentBatchFormatTest,SegmentBookieControlLogTest`：Arena控制、真实DATA格式/恢复及Bookie控制共83项定向测试通过，0失败/错误/跳过。此范围覆盖隔离物理append-only重开与既有owner/控制路径，不证明完整服务级可写restart或生产ACK。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest`：隔离真实文件定向测试27项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。物理append-only重开按已apply授权水位跳过旧块零尾，在新grant后写入另一stream，重开点读仍保留两者；同步失败、活跃writer、未apply控制命令和未授权原始写入均被拒绝。此入口不返回local result，未验证服务级可写重启、旧I/O错误持久分类或掉电。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制真实文件定向测试47项通过，0失败/错误/跳过。两个独立Java子进程分别在recovery DATA本地持久结果后及DATA force前`Runtime.halt`；父进程重放双控制日志、执行DATA恢复同步并读回唯一候选，局部比对保持物理证据状态。未执行掉电、生产ACK或可写重启验证。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制真实文件定向测试45项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。只读重开后recovery物理候选比对覆盖同步失败、相同payload及LAC变化、累计length冲突、第二stream generation、重复坐标和同步中close切断结果资格；旧force失败后即使完整bytes经重启同步可比对，也仅返回物理候选事实。未执行进程强制终止、掉电或生产网络/ACK验证。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制真实文件定向测试41项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。grant/close的慢force由预留控制I/O执行时owner仍推进，grant仅在回owner apply后开放准入，close在pre-cut票据终结后force且仅在apply后发布局部控制事实；执行前I/O拒绝保留cut并可无跳号重试，异步force失败不重开准入。尚未验证进程崩溃窗口、完整启动、真实网络或生产ACK。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制真实文件定向测试32项通过，0失败/错误/跳过。explicit LAC的慢force经预留控制I/O执行时，原owner仍可处理控制事件，结果回owner后才可见；执行器在I/O开始前拒绝时，撤销预留且同一请求可无跳号重试。此结果仍不证明真实网络、control group commit或生产read/ACK。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制真实文件定向测试30项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。explicit LAC验证inactive/错误凭据拒绝、单调更新与同值body冲突、fence后精确重试及新值拒绝、tombstone后不可读、重开重放、force结果不确定时原调用失败而完整记录经恢复同步后可见，并在受控慢force期间保持fence等待。尚无真实网络授权、group commit、confirmed-read/piggyback合并或掉电验证。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：隔离Bookie控制与真实文件定向测试27项通过，0失败/错误/跳过。重开后同batch双ledger仅保留未tombstone的本地读候选；被筛除record的损坏仍拒绝恢复。同轮`SegmentBatchFormatTest`的25项也通过，`bookkeeper-server` Spotless/Checkstyle通过。此为只读本地route筛选，不证明完整selector、对外读取授权、崩溃/掉电或可写restart。

2026-09-29 运行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=SegmentBatchFormatTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：格式、真实文件扫描与热目录相关25项测试通过，0失败/错误/跳过；同模块Spotless/Checkstyle通过。两个不同generation的stream各自扫描同步后作为一组装入时，容量不足或整组重复坐标均不留下先通过的候选，合法两项才一起可见。仍由调用方选定stream；此测试不证明必要DATA选择、selector/pin或可写restart。

2026-09-29 运行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=SegmentBookieControlLogTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：隔离Bookie控制与真实文件定向测试26项通过，0失败/错误/跳过；同模块Spotless/Checkstyle通过。已安装但未激活及durable fence后的局部读票据、正常关闭后重开点读、pending/durable tombstone切断读准入和重放拒绝均已覆盖。物理点读仍可看到被tombstone的bytes；这个局部门槛不验证网络读取权限、current-selector/pin、崩溃/掉电或完整可写restart。

2026-09-29 执行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=SegmentBatchFormatTest,SegmentBookieControlLogTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：隔离真实文件测试45项通过，0失败/错误/跳过；同模块`spotless:check checkstyle:check`通过。修复前的关闭反例确认仅剩未执行reservation时文件锁会被提前释放；修复后关闭拒绝该状态，I/O服务执行前拒绝任务时预留计数终结而物理坐标保留，后续预留不复用原范围。伪造“未force却完成”的既有测试在断言前缀失败后显式终结其测试用预留。尚无可写重开、旧I/O错误持久分类或掉电证据。

2026-09-29 执行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=SegmentBookieControlLogTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：20项通过，0失败/错误/跳过；同模块`spotless:check checkstyle:check`通过。同坐标重复请求在原DATA force期间停止等待，原写入及另一等待者仍完成；已发布候选的点读暂停期间停止等待，Bookie票据仍阻止fence越过读取，I/O与owner终结后fence才持久化。没有网络deadline/断连接线或生产ACK验证。

2026-09-29 执行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=SegmentBookieControlLogTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：19项通过，0失败/错误/跳过；同模块`spotless:check checkstyle:check`通过。真实DATA force挂起时停止一条已提交normal请求的响应等待，原Bookie票据仍有效，同坐标不同payload重试冲突；force完成后，同批另一请求得到`LOCAL_DURABLE`，已停止等待的请求无回调，相同payload重试命中已发布候选。此验证未接网络deadline/断连，也不证明可写重启或生产ACK。

2026-09-29 本次执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest,SegmentBookieControlLogTest`：隔离真实文件定向测试41项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。错shard的normal批次在提交前释放票据且不占DATA坐标；物理热候选在预留前拒绝同样的记录，即使底层原始格式路径写入完整字节，点读和重启扫描仍拒绝，扫描不执行恢复同步。合法同批两个ledger改用固定映射到同一shard的44/48；历史ledger7/shard0固定向量仅为framing oracle，原字节及历史回执未修改。尚未接权限化selector或生产ACK。

2026-09-29 本次执行`scripts/unified-wal/run-local-storage-tests.sh SegmentArenaControlLogTest,SegmentBatchFormatTest`：隔离真实文件定向测试31项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。未apply的两块pool尚不推进授权水位；重开重放后即使DATA仍为零，水位也包含两块，条件失败不推进，后续成功授权才推进。同步后的只读物理清单和stream候选报告这一水位。没有开放可写重启；已知旧I/O错误、完整末端前缀及掉电仍未验证。

2026-09-29 本次执行`scripts/unified-wal/run-local-storage-tests.sh SegmentControlFormatTest,SegmentArenaControlLogTest,SegmentBatchFormatTest,SegmentBookieControlLogTest`：隔离真实文件及固定向量定向测试51项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。Arena控制记录持久首序号7在未apply重开后恢复，后续分配改写为8按条件失败；错误前缀起点在预留DATA坐标前拒绝，重启候选用已apply首序号11核对调用方输入。192字节独立控制向量覆盖48字节ALLOC语义body，旧40字节语义记录明确拒绝重开。仍未验证空间used/unused、跨重启末端前缀、可写重启或掉电。

2026-09-28 本次执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest,SegmentBatchFormatTest`：隔离真实文件定向测试39项通过，0失败/错误/跳过。慢force期间，同坐标同内容重试等待原结果，不同内容立即冲突；原force成功后等待者取得`SAME_DURABLE`，失败后取得`UNKNOWN`。已发布候选的不同LAC同payload、不同payload、累计length冲突及坏CRC仍沿实际owner/I/O/点读路径验证；受控点读被延迟时更新activation generation，完成时返回`STALE_ADMISSION`；重启扫描候选没有运行期durable标记。尚未验证整体内存字节上限、可写restart或掉电。

2026-09-28 本次执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest,SegmentBatchFormatTest`：隔离真实文件定向测试38项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle检查通过。同批两条entry在一次force后按各自准入generation分别得到`STALE_ADMISSION`与`LOCAL_DURABLE`，较早物理前缀失败的回调只得到`UNKNOWN`；这不覆盖请求级幂等、同批明确fence失败、可写restart、掉电或生产ACK。

`unified-wal-local-TrhdAZFy`：20模块构建成功，`IsolatedSegmentConfigTest` 2项通过，0失败/错误/跳过；JUnit耗时0.033秒，Maven总耗时17.896秒（离线构建），两者都不是性能测量。Spotless和Checkstyle通过。source/config/run及原始输出校验见[步骤1回执](step1-receipt.json)。

2026-09-26 `unified-wal-local-Gms0z3fD`：14项通过（2配置、9 DATA/文件、3控制framing），0失败/错误/跳过，重试关闭；Spotless/Checkstyle通过。[步骤3回执](step3-receipt.json)绑定本次源码、命令和结果。覆盖固定向量、N-1/N/N+1、44字节差集、长度溢出、截断/坏CRC/未知类型/generation、短写/force失败/零进展、部分初始化、满额停止和单record读取。

文件测试的可复现工作量：默认DATA预创建2×16 MiB；4096字节批次在每次最多7字节的故障注入下需要586次write及1次force；1024字节payload的record为1220字节，先读160字节header再读record共1380字节，小于整批4096字节。16 KiB文件扣除4096字节身份块后容纳3个4096字节批次，下一批被拒绝。上述为工作量/边界测量，不是性能对照；首份局部性能报告等待步骤5～7的实际闭环。

2026-09-26 `unified-wal-local-Kjwv4bjm`：当前隔离测试20项通过，0失败/错误/跳过，Spotless/Checkstyle通过。[owner循环回执](owner-loop-receipt.json)记录本次源码和范围。新增6项测试验证满额4批仍可完成、控制队列保留容量、多shard轮转、无新输入deadline触发、实际FileChannel I/O线程与owner线程隔离、有界I/O拒绝/错误回传、异常后在途义务保留及100次空队列唤醒交错。I/O测试固定1线程＋1队列槽，第三次提交被拒绝并回原owner，不无限排队。测试工作量不证明CPU或延迟收益。

显式绑定验证见[cpu-binding回执](cpu-binding-receipt.json)。macOS Maven测试11项：10通过，1个Linux原生测试按平台跳过。Linux aarch64容器中以UID 65534、cpuset=`0,2`、quota=1 CPU、512 MiB运行同一Java测试字节码，现场用gcc构建当前JNI：10通过，1个非Linux测试按平台跳过。真实初始/恢复mask均为`{0,2}`，测试验证单CPU绑定、子线程继承、非法native mask拒绝和恢复。稀疏高编号、内核缩小mask、恢复失败和运行期mask变化用kernel boundary模型覆盖，不冒充硬件结果。Spotless/Checkstyle通过。

复现Java/native模块检查（Linux上会执行原生绑定测试）：

```bash
mvn -o -pl cpu-affinity -am -Dtest=CpuBindingTest,CpuInfoTest,IsolatedProcessorsTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -pl cpu-affinity -DskipTests spotless:check checkstyle:check
```

容器的完整编译/测试命令、image ID、classpath与源码/字节码hash保存在回执中。此处是Linux JNI路径证据，不是Linux Maven全构建、NUMA硬件、吞吐或绑核收益证据。

拓扑/placement验证见[本次回执](topology-placement-receipt.json)：macOS 18项中16通过、2个Linux测试按平台跳过；Linux容器18项中17通过、1个非Linux测试按平台跳过，Spotless/Checkstyle通过。固定fixture为2 socket、3物理核、6逻辑CPU，可见祖先quota为3.5 CPU；2个append worker保留1个服务物理核。另覆盖SMT冲突、非连续集合、缺失quota、异常sibling、允许内存节点及mount路径不能解析。实际容器只读结果为CPU `{0,2}`、quota=1、memory nodes `{0}`，测试逐项对照容器约束；不能据此声称物理NUMA或性能收益。

```bash
mvn -o -pl cpu-affinity -am -Dtest=CpuTopologySnapshotTest,CpuBindingTest,CpuInfoTest,IsolatedProcessorsTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
```

group启动与池验证见[本次回执](execution-startup-receipt.json)：2026-09-26 `unified-wal-local-0lK9Qwxl` 为26通过、1个Linux测试按平台跳过；Linux容器运行owner/group测试为12通过、1个非Linux测试按平台跳过，Spotless/Checkstyle通过。Linux fixture固定UID 65534、cpuset `0,2,4`、quota=3 CPU，验证REQUIRE绑定后才初始化池、main线程mask不变和部分初始化失败后全部线程退出且恢复无错误。macOS验证native不可用时PREFER fallback及REQUIRE拒绝。

本次资源测量固定2个owner、1个I/O线程、5个pending按3/2分配、2个控制槽、3个buffer按2/1分配；每个buffer可用4096字节、direct请求容量8191字节，共24573字节，没有按owner或NUMA节点复制总预算。覆盖DATA满额时归还、池耗尽、重复归还、借出buffer阻止提前关闭和返回后继续drain。未测吞吐、页分布或NUMA收益。

2026-09-28 [运行期回执](runtime-placement-receipt.json)：macOS执行组/owner/配置19项中14通过、5项Linux专属测试跳过；CPU绑定/拓扑18项中16通过、2项Linux专属测试跳过。Linux非root容器固定cpuset `{0,2,4}`、quota=3 CPU、512 MiB，现场构建JNI并运行35项，32通过、3项不同fixture或非Linux测试跳过；另以quota=2 CPU执行PREFER启动退回OFF的1项测试通过。真实native路径验证绑定后重读部署额度、owner实际mask在运行中经native改动后REQUIRE停止准入；quota下降由构造的拓扑快照注入，验证PREFER在原线程释放绑定、REQUIRE保留既有ticket并等待完成，不冒充真实cgroup在线重配置。两个受影响模块的Spotless/Checkstyle通过。绑定计划额外占1个低频监测服务线程；池、pending和control预算未复制。未测延迟、吞吐或物理NUMA页属地。

2026-09-28 [Arena控制回执](arena-control-receipt.json)：macOS隔离真实文件测试中，Arena控制4项、既有framing3项均通过，0失败/错误/跳过；`bookkeeper-server`的Spotless/Checkstyle通过。覆盖同批争用、durable未apply后重放、重复及不兼容命令、force失败的unknown恢复、partial tail拒绝、身份不符和容量上限。`ALLOC_POOL`只证明空间owner，不证明DATA used/unused、普通Add或掉电后的成功确认。

2026-09-28 Bookie INSTALL/ACTIVATE/FENCE局部测试：运行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`，macOS真实FileChannel测试15项通过，0失败/错误/跳过；`bookkeeper-server`的Spotless/Checkstyle通过。测试覆盖安装与激活分离、同批一次force和顺序apply、未apply/force不确定后的重开重放、同操作冲突和等价重试、错误authority/密钥/storage identity、损坏尾部及日志满额。票据测试覆盖fence切断新准入、等待pre-cut票据的物理终结、先durable的ACTIVATE仍按日志顺序apply、重复fence、慢force期间局部准入查询及时拒绝与重启后保持fenced。新增真实DATA FileChannel测试验证慢force时fence等待、先失败批次使后批提前回调但不提前释放仍在I/O中的票据，以及冻结record与票据一一匹配。控制owner测试验证INSTALL/ACTIVATE及FENCE经预留I/O force后回owner apply、等待和慢force期间owner可推进、控制I/O拒收后未写FENCE序号可安全重试。只证明隔离文件与物理I/O的顺序，不证明真实mTLS/MetadataStore授权、请求级local result、进程掉电或生产Add成功。

2026-09-28 [控制owner回执](arena-control-owner-receipt.json)：macOS隔离测试27项通过，0失败/错误/跳过，Spotless/Checkstyle通过。1个DATA任务占满其准入额度、I/O服务有空位时，1个Arena控制任务仍可保留独立completion，真实写入并force控制日志，再由原owner按序apply；控制额度满拒绝第二项，停收后等待在途控制和DATA终结，重开仍重放授权。共享I/O队列无控制专属余量，尚未接入Bookie服务生命周期、Bookie级控制owner或生产请求。

2026-09-28 [控制I/O余量回执](control-io-reserve-receipt.json)：macOS隔离测试23项计数中18项执行通过、5项因平台条件跳过，0失败/错误；旧I/O构造路径的BatchFormat另16项通过，Spotless/Checkstyle通过。组内I/O线程总额分出1个控制服务及独立队列，DATA工作线程和队列同时占满时，Arena控制仍在另一线程真实写入并force，再回原owner apply；组启动、控制提交和排空路径通过定向测试。未接入实际Bookie服务驱动，不证明控制服务本身I/O故障后的可用性、生产ACK或掉电语义。

2026-09-28 [Arena控制驱动回执](arena-control-driver-receipt.json)：macOS隔离真实文件测试7项通过，0失败/错误/跳过，Spotless/Checkstyle通过。驱动一次只接收一个batch；force成功后原owner apply并回传结果，force故障返回错误且不授予grant；重开可重放成功授权。尚未接入Bookie启动、停止、路由/权限或生产Add。

2026-09-28 [Arena控制锁边界回执](arena-control-lock-receipt.json)：macOS隔离真实文件测试24项通过，0失败/错误/跳过，Spotless/Checkstyle通过。控制文件force被确定性挂起时，grant查询与第二批拒绝均及时返回，close拒绝关闭在途文件；force释放后才可apply授权。未验证DATA文件writer的非阻塞预留或Bookie服务路径。

2026-09-28 [DATA文件锁边界回执](data-file-lock-receipt.json)：macOS隔离真实文件测试25项通过，0失败/错误/跳过，Spotless/Checkstyle通过。DATA文件force被确定性挂起时，下一批范围预留仍及时返回，close拒绝关闭在途文件；释放后两批均由真实FileChannel写入并完成force。未验证Bookie权限、逐entry结果或掉电语义。

2026-09-28 [DATA授权回执](arena-data-grant-receipt.json)：macOS隔离测试17项通过，0失败/错误/跳过，Spotless/Checkstyle通过。真实FileChannel路径用1 MiB payload生成跨两个1 MiB块的冻结batch；未apply、allocation ID/shard generation不符、另一隔离目录的grant均在写入前拒绝，同一pool授权后写入并由独立parser读回。Arena局部测试另覆盖单块grant不授权跨块范围及allocation ID重复争用。未测Bookie route/fence、重启DATA used/unused和逐entry结果。

2026-09-28 [只读DATA扫描回执](data-scan-receipt.json)：同一局部测试扩展为18项通过，0失败/错误/跳过，Spotless/Checkstyle通过。停写后重开Arena控制日志，再以目录锁扫描DATA；完整已授权batch被验证，活跃writer、尾部非零脏页和完整但没有控制grant的原始batch被拒绝。全零页数不用于判定可复用空间。

2026-09-28 [物理前缀回执](physical-prefix-receipt.json)：macOS隔离测试24项通过，0失败/错误/跳过，Spotless/Checkstyle通过。真实预创建文件中，先预留sequence 11/12、让12先write/force，确认11完成前不会释放12；注入11的force失败后，即使12的字节完整也不释放其物理候选。另验证没有对应reservation的真实force返回时，空错误回调不能推进前缀。该结果不授予逐entry成功、重启起点或掉电保证。

2026-09-28 [点读回执](point-read-receipt.json)：macOS隔离测试19项通过，0失败/错误/跳过，Spotless/Checkstyle通过。真实文件内270336字节的三record batch中，目标record header横跨两个对齐页；测试确认只读目标范围而非整批、grant前或DATA写入前拒绝、错误entry ID和无界长度拒绝、record字节篡改后CRC拒绝。关闭writer并以更低的新写入阈值重开Arena日志后，旧entry仍可点读。此结果不授予热locator发布、pin、完整restart或掉电保证。

2026-09-28 [恢复同步回执](data-recovery-sync-receipt.json)：macOS隔离测试19项通过，0失败/错误/跳过，Spotless/Checkstyle通过。停写后重开Arena控制日志，完整已授权DATA经扫描后对该文件执行一次真实`force(false)`；注入force失败时不返回成功结果。无grant或尾部脏页在force之前拒绝。此为文件同步和失败边界证据，不证明已选出全部必要DATA、重建前缀、恢复entry权限或掉电后的耐久性。

2026-09-28 [物理热候选回执](hot-directory-receipt.json)：macOS隔离测试26项通过，0失败/错误/跳过，Spotless/Checkstyle通过。真实DATA文件中12先写并force仍无候选，11完成后11/12一起发布、12可从候选点读；11的force失败则候选保持未发布。两个候选占满硬上限后，第三个batch在DATA坐标前被拒绝；单独的搬迁副本完成物理前缀却不发布普通候选。未验证Bookie权限、逐entry成功、幂等、重启索引重建或性能。

2026-09-28 [重复慢路径回执](duplicate-check-receipt.json)：macOS隔离定向测试26项通过，0失败/错误/跳过，Spotless/Checkstyle通过。已发布候选从真实DATA文件读取，与新打包的相同payload/累计length但不同LAC比较相同；不同payload或累计length比较冲突，坏CRC、不同instance及entryId拒绝。该原语未连接Bookie authority或请求生命周期，不能证明同坐标Add幂等成功。

2026-09-28 [重启物理清单回执](physical-inventory-receipt.json)：macOS隔离定向测试26项通过，0失败/错误/跳过，Spotless/Checkstyle通过。两个已force的真实DATA batch在writer退出、Arena控制日志重开后，经全目录校验与一次恢复`force(false)`得到两个物理位置，并再次点读11/12；清单容量1和注入同步失败均不返回结果。单独的持久搬迁副本不进入普通清单。此处没有持久前缀起点、Bookie selector/权限或热目录恢复发布，不能返回幂等成功或宣称完整restart。

2026-09-28 [DATA物理驱动回执](data-physical-driver-receipt.json)：macOS隔离定向测试25项通过，0失败/错误/跳过，Spotless/Checkstyle通过。两个已授权batch经原owner预留、真实FileChannel write/force和连续前缀发布后均得到可点读候选；注入首批force失败时，两个未决回调均报错，候选不发布且坐标保留。该回调不是逐entry结果，未接入Bookie权限或生产ACK。

2026-09-28 [执行组排空回执](data-driver-drain-receipt.json)：macOS隔离定向测试32项中27执行通过、5项Linux专属测试跳过，0失败/错误；Spotless/Checkstyle通过。真实DATA force在隔离group关闭时保持在途，文件拒绝提前关闭；释放force后候选可点读，owner/I/O服务完成排空。尚未验证Bookie服务生命周期或掉电恢复。

2026-09-28 [物理stream候选回执](physical-stream-candidate-receipt.json)：macOS隔离真实文件测试21项通过，0失败/错误/跳过，Spotless/Checkstyle通过。已授权batch在writer退出、控制日志重开及DATA同步后，以显式起点11得到连续候选至12并可点读；缺11之前的起点、序号11/13缺口、重复11和batch容量不足均拒绝。该起点仍由调用方提供，未证明持久前缀或Bookie权限。

2026-09-28 [物理合批回执](physical-batch-receipt.json)：macOS隔离定向测试34项中29执行通过、5项Linux专属测试跳过，0失败/错误；Spotless/Checkstyle通过。同一冻结DATA batch的两条不同entry经隔离group的owner/I/O路径完成一次真实FileChannel force，连续物理前缀释放后返回两个可点读候选。未实现逐entry local result或Bookie ACK。

2026-09-28 本轮执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBatchFormatTest`：22项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。真实文件关闭writer、重开控制日志、扫描同步后，候选在新owner的有界热目录恢复并再次点读；容量不足和重复恢复均不产生部分发布。此处没有持久起点、Bookie selector/权限或可写重启证据。

2026-09-28 scope复核执行`scripts/unified-wal/run-local-storage-tests.sh IsolatedSegmentConfigTest,SegmentBatchFormatTest`：24项通过，0失败/错误/跳过。恢复候选装入新owner热目录前拒绝错误目录或storage identity；运行时物理驱动也拒绝与DATA文件不匹配的热目录。同一持久scope调整新写payload阈值后仍可装入旧候选。此为隔离路径的误接线防护，不证明Bookie selector或生产恢复权限。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest`：50项通过，0失败/错误/跳过。normal DATA得到隔离本地结果并关闭重开后，持普通准入票据同步并点读相同/冲突物理候选；同步失败释放票据，tombstone在同步期间撤权使结果为`STALE_ADMISSION`。这只验证物理比较和票据生命周期，未证明current selector、旧I/O错误分类或durable duplicate成功。

2026-09-29 同一命令执行52项通过，0失败/错误/跳过。新增两个普通Add子进程边界：本地结果后、完整写入但barrier前分别`Runtime.halt`；父进程重放双控制日志、同步目标DATA、点读并匹配普通重试物理候选。`Runtime.halt`不模拟掉电，测试没有返回重启幂等成功或生产ACK。

2026-09-29 执行`scripts/unified-wal/run-local-storage-tests.sh SegmentBookieControlLogTest,SegmentBatchFormatTest`：80项通过，0失败/错误/跳过。无MOVE/FREE且单stream时，隔离恢复点读持Bookie读票据与DATA writer排他锁，经同步、唯一物理位置选择、点读及末尾route/stream复核返回entry；普通/recovery候选比较也在同一锁内完成。测试覆盖锁在点读期间阻止writer重开、回调异常后释放锁、未知坐标不报absence、多stream拒绝选择，以及同步期间tombstone撤权。一般selector/pin、服务级读取、掉电与ACK仍未验证。

同轮另执行`scripts/unified-wal/run-local-storage-tests.sh SegmentArenaControlLogTest`：9项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。此检查覆盖受共同文件锁重构影响的Arena控制与既有扫描调用，不扩展为完整restart验收。

## 未覆盖范围

步骤4的Bookie生命周期自动重启、空闲I/O线程即时释放、PREFERRED页策略及存储集成、步骤5 Bookie控制剩余状态及Arena/DATA恢复闭环、步骤6完整合批准入/权限化热定位/生产逐entry结果/请求级幂等、步骤7可发布末端前缀、selector与pin/完整restart尚未实现。物理热候选及其只读重建、重复比较、点读和文件同步的隔离局部实现不等于步骤6或7完成。性能、物理NUMA、掉电、真实网络/MetadataStore和Gate验证均NOT_EXECUTED。局部文件测试不授予生产authority。
