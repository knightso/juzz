# juzz
framework to connect and run sequential goroutines.

## example

### run

```go
var ws juzz.Workers

starter := ws.NewStarter("start", start, 1)
composer := ws.NewWorker("compose", compose, 1)
renderer := ws.NewWorker("render", render, 10)
uploader := ws.NewWorker("upload", upload, 1)

observer.Next(scenesGetter).Next(mp4Getter).Next(scenesFlipper).Next(sceneFlipsPutter)

ws.Run()
```

### graceful shutdown

```go
ws.Shutdown()
```
