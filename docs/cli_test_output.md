# CLI 動作確認ログ

## 1. リポジトリ初期化

```
cargo run --example cli -- init
```

```
Initialized CRSL repository at "./crsl_data"
```

---

## 2. コンテンツ作成

```
cargo run --example cli -- create --content "Hello World" --author alice
```

```
Created content:
  Content ID: bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
  Version: bafkreiehts7g6jsvanl7ir3cryzfjnnoa357va4khra6pptxdkppaq7xcm
```

---

## 3. コンテンツ表示

```
cargo run --example cli -- show bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
```

```
Content ID: bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
Content: Hello World
```

---

## 4. コンテンツ更新

```
cargo run --example cli -- update --genesis-id bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny --content "Updated Hello World" --author bob
```

```
Updated content:
  Genesis ID: bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
  Version: bafkreidpig3ralhmywkcog6tcjandvqixj24oaelmu7sf7qq5bnqueufgm
```

---

## 5. 更新後のコンテンツ表示

```
cargo run --example cli -- show bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
```

```
Content ID: bafkreiffsgtnic7uebaeuaixgph3pmmq2ywglpylzwrswv5so7m23hyuny
Content: Updated Hello World
``` 