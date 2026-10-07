package main

import (
	"embed"
	"fmt"
	"log"
	"os"
	"strings"

	streamjoin "github.com/TommyMarsss/stream-join-watermark"
)

//go:embed template.html
var templateFS embed.FS

type arrival struct {
	side streamjoin.Side
	key  string
	t    int64
}

// scenario 与 README“回放场景说明（18 帧）”完全一致：
// 窗口长度 10、乱序容忍 3；最终 7 条 Join 结果、3 条迟到侧输出。
func scenario() []arrival {
	return []arrival{
		{streamjoin.Left, "a", 2},  // 1  进入 [0,10)
		{streamjoin.Right, "a", 4}, // 2  配对 (2,4) 暂存
		{streamjoin.Right, "a", 1}, // 3  容忍内乱序，再配对 (2,1)
		{streamjoin.Left, "x", 13}, // 4  WM=10 关闭 [0,10)，输出 2 条
		{streamjoin.Right, "b", 11},
		{streamjoin.Left, "b", 15},
		{streamjoin.Right, "x", 18},
		{streamjoin.Right, "a", 3}, // 8  迟到 -> 侧输出
		{streamjoin.Left, "c", 25}, // 9  关闭 [10,20)，输出 2 条
		{streamjoin.Left, "a", 19}, // 10 迟到 -> 侧输出
		{streamjoin.Right, "c", 28},
		{streamjoin.Right, "b", 20}, // 12 边界事件入 [20,30)
		{streamjoin.Right, "c", 22}, // 13 乱序但未迟到
		{streamjoin.Left, "c", 35},  // 14 关闭 [20,30)，输出 2 条
		{streamjoin.Right, "b", 21}, // 15 迟到 -> 侧输出
		{streamjoin.Right, "c", 36},
		{streamjoin.Right, "c", 40}, // 17 边界事件入 [40,50)
		{streamjoin.Left, "z", 43},  // 18 WM=40 关闭 [30,40)，输出 1 条
	}
}

func main() {
	out := "watermark-demo.html"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}

	j := streamjoin.NewJoiner(10, 3)
	for _, a := range scenario() {
		j.Process(a.side, a.key, a.t)
	}
	data, err := j.MarshalReplay()
	if err != nil {
		log.Fatalf("序列化回放数据失败: %v", err)
	}

	tmpl, err := templateFS.ReadFile("template.html")
	if err != nil {
		log.Fatalf("读取嵌入模板失败: %v", err)
	}
	page := strings.Replace(string(tmpl), "/*__REPLAY_DATA__*/", string(data), 1)

	if err := os.WriteFile(out, []byte(page), 0o644); err != nil {
		log.Fatalf("写入 %s 失败: %v", out, err)
	}

	fmt.Printf("已生成 %s：%d 帧，%d 条 Join 结果，%d 条迟到侧输出\n",
		out, len(j.Frames()), len(j.Results()), len(j.LateEvents()))
}
