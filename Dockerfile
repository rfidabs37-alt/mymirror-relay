FROM dart:stable AS build
WORKDIR /app
COPY relay.dart .
RUN dart compile exe relay.dart -o /app/relay

FROM scratch
COPY --from=build /runtime/ /
COPY --from=build /app/relay /app/relay
ENV HOST=0.0.0.0 PORT=8080
EXPOSE 8080
ENTRYPOINT ["/app/relay"]
