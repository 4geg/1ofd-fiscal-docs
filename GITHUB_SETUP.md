# Публикация на GitHub — пошагово

Самый простой вариант без GitHub Desktop.

## 1. Создай пустой репозиторий

1. Открой github.com и войди в аккаунт.
2. Нажми **New repository**.
3. Имя, например: `1ofd-fiscal-docs`.
4. Выбери **Private** или **Public**.
5. **Не** ставь галочки `Add a README`, `.gitignore`, `license` — они уже есть в проекте.
6. Нажми **Create repository**.
7. На следующей странице скопируй HTTPS-адрес репозитория, например `https://github.com/USERNAME/1ofd-fiscal-docs.git`.

## 2. Распакуй проект

Распакуй архив проекта в обычную папку, например:

`C:\Projects\1ofd-fiscal-docs`

## 3. Открой папку в VS Code

**File → Open Folder →** выбери папку проекта.

Открой **Terminal → New Terminal**.

## 4. Выполни команды

По одной строке:

```powershell
git init
git add .
git commit -m "Release 1.7.2"
git branch -M main
git remote add origin https://github.com/USERNAME/1ofd-fiscal-docs.git
git push -u origin main
```

В четвёртой команде замени URL на адрес своего репозитория.

Если Git попросит имя и почту:

```powershell
git config --global user.name "4geg"
git config --global user.email "ТВОЯ_ПОЧТА"
```

После этого снова выполни `git commit -m "Release 1.7.2"`.

## 5. Получи готовый EXE через GitHub

Самый простой путь:

1. Открой репозиторий на GitHub.
2. Вкладка **Actions**.
3. Слева **Build Windows EXE**.
4. **Run workflow → Run workflow**.
5. Подожди окончания сборки.
6. Открой готовый запуск workflow.
7. Внизу страницы скачай artifact `1OFD_FiscalDocs-v1.7.2-Windows`.

## 6. Сделай Release 1.7.2

В терминале VS Code:

```powershell
git tag v1.7.2
git push origin v1.7.2
```

После отправки тега GitHub Actions автоматически соберёт EXE и прикрепит его к GitHub Release.
