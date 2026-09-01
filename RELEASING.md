# 发布指南

本文档面向项目维护者。使用 Starter 的业务项目不需要配置 Central Token、GPG 或额外 Maven 仓库。

## 发布前提

发布前确认以下条件全部满足：

- Central Portal 中的 `com.zaviwayne` 命名空间状态为 `Verified`。
- Central Portal 已创建 User Token，且 Token 所属账号拥有该命名空间的发布权限。
- 本机存在可用于签名的 GPG 私钥。
- 对应公钥已经上传到 Central 支持的公共 PGP Key Server，并且可以通过完整指纹检索。
- 待发布版本未曾发布到 Maven Central。Maven Central 上的版本不可覆盖。

## Central 凭据

在用户级 `~/.m2/settings.xml` 中配置服务器。`id` 必须与根 POM 中的 `publishingServerId` 一致：

```xml
<settings>
    <servers>
        <server>
            <id>central</id>
            <username>${env.CENTRAL_USERNAME}</username>
            <password>${env.CENTRAL_TOKEN}</password>
        </server>
    </servers>
</settings>
```

Token 用户名和密码由 Central Portal 生成，不是 Central 登录邮箱或 GitHub 用户名。发布前在当前终端会话
中设置对应环境变量，不要将真实凭据写入项目：

```powershell
$env:CENTRAL_USERNAME = "Central Token 用户名"
$env:CENTRAL_TOKEN = "Central Token 密码"
```

也可以直接在本机 Maven `settings.xml` 中配置 Central Token。无论采用哪种方式，都不得提交包含真实
凭据的 `settings.xml`。

## GPG 签名

首次发布前生成密钥，并记录 `sec` 行中的 Key ID 和完整指纹：

```bash
gpg --full-generate-key
gpg --list-secret-keys --keyid-format LONG
gpg --fingerprint YOUR_KEY_ID
```

建议使用 RSA 4096，并为私钥设置强口令。将公钥上传到公共 Key Server：

```bash
gpg --keyserver hkps://keyserver.ubuntu.com --send-keys YOUR_KEY_ID
```

仅上传公钥，不得上传私钥。确认公钥可以通过完整指纹公开检索后，再等待 Central 的 Key Server 缓存
完成同步。自动化或无法交互输入口令时，可以在当前 PowerShell 会话设置：

```powershell
$securePassphrase = Read-Host "GPG passphrase" -AsSecureString
$env:MAVEN_GPG_PASSPHRASE = [System.Net.NetworkCredential]::new("", $securePassphrase).Password
```

## 更新版本

发布前在仓库根目录更新根 POM 和所有模块父版本：

```bash
./mvnw versions:set -DnewVersion=NEW_VERSION -DprocessAllModules=true -DgenerateBackupPoms=false
```

然后手动更新 README 中的依赖版本。检查全部变更，确认所有模块使用同一个非 `SNAPSHOT` 版本。

## 验证发布包

先执行完整测试和发布构建：

```bash
./mvnw -Prelease clean verify
```

确认所有测试通过，且各模块生成的 POM、JAR、源码包和 Javadoc 包均存在对应 `.asc` 签名。

## 发布到 Central

从仓库根目录执行完整 Reactor 发布：

```bash
./mvnw -Prelease clean deploy
```

Central 发布失败时，不要使用 Maven 提示的 `-rf` 从单个模块恢复。修复命名空间、签名或元数据问题后，
重新执行完整的 `clean deploy`，确保父 POM 和所有模块进入同一个部署。

在 Central Portal 的 Deployments 页面确认部署最终进入 `PUBLISHED` 状态。如果部署停留在
`VALIDATED`，按页面提示完成发布确认；如果进入 `FAILED`，先查看每个制品的具体校验错误。

## 发布后验证

等待 Maven Central 同步完成后，直接检查公开 POM：

```text
https://repo.maven.apache.org/maven2/com/zaviwayne/async-task-spring-boot-starter/NEW_VERSION/async-task-spring-boot-starter-NEW_VERSION.pom
```

确认父 POM、Starter 及所有传递模块均可匿名访问后，再创建并推送版本标签：

```bash
git tag vNEW_VERSION
git push origin vNEW_VERSION
```

`search.maven.org` 的搜索索引可能晚于 Maven Central 仓库同步，不应只根据搜索结果判断发布是否可用。
