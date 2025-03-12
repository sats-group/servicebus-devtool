dotnet run -- --help
```
Copyright (C) 2025 servicebus-devtool

  peek       Peek messages from a queue or topic subscription.

  add        Adds messages from stdin to a service bus queue/topic.

  help       Display more information on a specific command.

  version    Display version information.

```

dotnet run -- peek --help
```
servicebus-devtool 1.0.0
Copyright (C) 2023 servicebus-devtool

  --messages      The number of messages to peek.

  --delete        If set, the messages are removed from the queue.

  --help          Display this help screen.

  --version       Display version information.

  src (pos. 0)    Required. The name of the source queue from which the messages will be peeked from.
```

# Example usage

`file.csv`:
```csv
157p220548
222p179793
161p201824
802p53216
147p271760
156p277267
253p127635
801p179604
161p207128
157p220612
152p250883
801p182001
```

```sh
awk '{ split($0,id,"p"); print "{\"bookingId\": \"" id[1] "book" id[2] "\", \"bookingState\": \"Planned\" }"; }' ./file.csv \
  | dotnet run -- add -t 0.2 'namespace.servicebus.windows.net/queue'
```
