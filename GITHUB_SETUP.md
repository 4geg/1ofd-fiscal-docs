# Обновление проекта и выпуск GitHub Release

Текущий репозиторий: `4geg/1ofd-fiscal-docs`.

## Важно: автор коммита только 4geg

Перед **каждым релизным коммитом** в локальной папке репозитория выполни:

```powershell
git config --local user.name "4geg"
git config --local user.email "264871936+4geg@users.noreply.github.com"
git config --local user.name
git config --local user.email
```

В выводе должны быть только:

```text
4geg
264871936+4geg@users.noreply.github.com
```

Так локальная папка репозитория не сможет снова взять старого автора из другой Git-конфигурации.

## Если репозиторий уже есть на компьютере

1. Распакуй обновление поверх локальной папки репозитория с заменой файлов.
2. Открой эту папку в VS Code.
3. Выполни проверку автора выше, затем:

```powershell
git status
git add .
git commit -m "Release 1.7.5"
git log -1 --format="%h | %an <%ae> | %cn <%ce>"
git push
```

Последняя строка `git log` должна содержать `4geg <264871936+4geg@users.noreply.github.com>` и в Author, и в Committer. Если там другое имя — **не пушить**.

4. При необходимости сначала проверь тестовую сборку через **GitHub → Actions → Build Windows EXE → Run workflow**.
5. После проверки создай официальный Release:

```powershell
git tag -a v1.7.5 -m "1OFD Fiscal Docs 1.7.5"
git push origin v1.7.5
```

Push тега запускает workflow. Он собирает `1OFD_FiscalDocs.exe`, создаёт/обновляет Release `v1.7.5` и прикрепляет:

- `1OFD_FiscalDocs.exe`
- `SHA256SUMS.txt`

Прямая ссылка из README всегда ведёт на EXE последнего Release:

`https://github.com/4geg/1ofd-fiscal-docs/releases/latest/download/1OFD_FiscalDocs.exe`

## Если локальной папки репозитория больше нет

Не делай новый `git init`. Клонируй существующий репозиторий:

```powershell
cd C:\Users\peshka_Linkkoma\Desktop
git clone https://github.com/4geg/1ofd-fiscal-docs.git
cd .\1ofd-fiscal-docs\
```

После клонирования обязательно снова выполни локальные `git config --local ...` из первого раздела, затем скопируй содержимое полного архива 1.7.5 с заменой файлов.

## Проверка перед тегом

Минимум проверь:

- автор последнего коммита — `4geg`;
- первый запуск EXE и автооткрытие интерфейса;
- стрелки у всех select после раскрытия/закрытия;
- CSV/JSON кнопки и скачивание;
- sticky-заголовки таблицы при прокрутке;
- карточку ФД: scrollbar внутри окна;
- раскрытие **Дополнительные данные ФД** и **Полный ответ API**;
- кнопку **В новой вкладке** на 2–3 разных ФД;
- поиск и кнопку **Стоп**;
- восстановление процесса/результата после F5 и повторного открытия вкладки;
- пункт трея **Скопировать ссылку интерфейса**;
- `Перезапустить` и `Завершить`.
