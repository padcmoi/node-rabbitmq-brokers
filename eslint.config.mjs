import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default [
  {
    ignores: [
      "**/*.js",

      //
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
        ecmaVersion: 2022,
        sourceType: "module",
      },
      globals: {
        process: "readonly",
        console: "readonly",
        setTimeout: "readonly",
        module: "readonly",
        __dirname: "readonly",
      },
    },
    rules: {
      // Detects variables / arguments declared but not used
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          args: "all",
          argsIgnorePattern: "^_",
          caughtErrors: "all",
          caughtErrorsIgnorePattern: "^_",
          destructuredArrayIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
      // Disallow any explicit use of `any`
      "@typescript-eslint/no-explicit-any": "error", // error
      // unsafe-any usage
      "@typescript-eslint/no-unsafe-member-access": "error",
      "@typescript-eslint/no-unsafe-assignment": "error",
      "@typescript-eslint/no-unsafe-call": "error",

      // Warn on usage of `!` (non-null assertion)
      "@typescript-eslint/no-non-null-assertion": "warn",
      // Allow broad union typing without redundant-type blocking
      "@typescript-eslint/no-redundant-type-constituents": "off",
      // Enforce `import type` when the import is only for types
      "@typescript-eslint/consistent-type-imports": "warn",
      // Prevent incorrect Promise usage (missing await, etc.)
      "@typescript-eslint/no-misused-promises": ["error", { checksVoidReturn: false }],

      // Disallow async functions without await
      "@typescript-eslint/require-await": "error",
      // Do not enforce explicit function return types
      "@typescript-eslint/explicit-function-return-type": "off",
      "no-restricted-syntax": [
        "error",
        {
          selector: ":matches(FunctionDeclaration, FunctionExpression, ArrowFunctionExpression)[returnType]",
          message: "Avoid explicit return type annotations; prefer inference.",
        },
        {
          selector: "MethodDefinition[value.returnType]",
          message: "Avoid explicit return type annotations on methods; prefer inference.",
        },
        {
          selector: "Property[value.type=/Function/][value.returnType]",
          message: "Avoid explicit return type annotations on object methods; prefer inference.",
        },
      ],

      // Only allow console.warn / error / info
      "no-console": ["warn", { allow: ["warn", "error", "info"] }],
      // Detect unnecessary boolean casts (!!value)
      "no-extra-boolean-cast": "warn",
      // Detect unreachable code
      "no-unreachable": "error",
      // Detect unnecessary empty patterns
      "no-empty-pattern": "warn",
      // Detect conditions that are always true / false
      "no-constant-condition": "warn",
      // Disabled because handled by TypeScript
      "no-undef": "off",
    },
  },
];
