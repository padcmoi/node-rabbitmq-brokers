# <package-name>

> Template README — replace all placeholders before publishing.

## What is this package?

TODO: short purpose (1-2 lines).

## Install

```bash
npm i <package-name>
```

## Quick start

TODO: add minimal usage example.

## API

TODO: list exported functions/classes/types.

## Integration guides

- [Express](./docs/express.md)
- [NestJS](./docs/nestjs.md)

## Development

```bash
npm ci
npm run lint
npm run typecheck
npm test
npm run build
```

## Notes

TODO: add constraints, compatibility, and caveats.

Configuration rule:

- Never read `process.env` inside the library.
- Read env values in the app service/provider layer and pass plain config to the library.
