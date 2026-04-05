import json
import re
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError


STRICT_MODE_FORMAT_HELP = """Use this exact event post format (caption or message text):

Title: Your event title
Description: Short summary for cards
Overview: Detailed event overview
Venue: Exact venue name
Location: City, area, or address
Date: YYYY-MM-DD
Time: HH:MM (24-hour)
Mode: in-person | online | hybrid
Audience: Target audience
Organizer: Organizer name
Tags: tag1, tag2, tag3
Agenda:
- Item one
- Item two
- Item three

Attach one image/photo to the same Telegram post."""


def parse_csv_env(value: str) -> Iterable[str]:
	return [item.strip() for item in value.split(",") if item.strip()]


def parse_channel_filters(value: str) -> Tuple[Set[str], Set[str]]:
	ids: Set[str] = set()
	usernames: Set[str] = set()

	for item in parse_csv_env(value):
		if item.startswith("@"):
			usernames.add(item[1:].lower())
			continue

		if item.startswith("-100") or item.startswith("-") or item.isdigit():
			ids.add(item)
			continue

		usernames.add(item.lower())

	return ids, usernames


def load_aggregated_env_var(env_key: str = "WORKER_ENV_PAIRS") -> int:
	raw = os.getenv(env_key, "").strip()
	if not raw:
		return 0

	loaded_count = 0

	# Format 1: JSON object string
	if raw.startswith("{") and raw.endswith("}"):
		try:
			parsed = json.loads(raw)
			if isinstance(parsed, dict):
				for key, value in parsed.items():
					if not isinstance(key, str):
						continue
					os.environ[key] = "" if value is None else str(value)
					loaded_count += 1
				return loaded_count
		except json.JSONDecodeError:
			# Fall back to line-based parser
			pass

	# Format 2: KEY=VALUE pairs separated by newlines or semicolons
	normalized = raw.replace(";", "\n")
	for line in normalized.splitlines():
		entry = line.strip()
		if not entry or entry.startswith("#") or "=" not in entry:
			continue

		key, value = entry.split("=", 1)
		key = key.strip()
		value = value.strip().strip('"').strip("'")
		if not key:
			continue

		os.environ[key] = value
		loaded_count += 1

	return loaded_count


class TelegramEventWorker:
	def __init__(self) -> None:
		load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "local.env"))
		aggregated_loaded = load_aggregated_env_var("WORKER_ENV_PAIRS")
		if aggregated_loaded > 0:
			print(f"[INFO] Loaded {aggregated_loaded} env keys from WORKER_ENV_PAIRS")

		self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
		self.mongo_uri = os.getenv("MONGODB_URI", "").strip()
		self.mongo_db_name = os.getenv("MONGODB_DB", "test").strip() or "test"
		self.channels_value = os.getenv("TELEGRAM_CHANNELS", "").strip()
		self.keywords = [
			keyword.lower()
			for keyword in parse_csv_env(
				os.getenv(
					"TELEGRAM_KEYWORDS",
					"hackathon,meetup,conference,bootcamp,summit,workshop,webinar,event",
				)
			)
		]
		self.poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
		self.long_poll_timeout = int(os.getenv("TELEGRAM_LONG_POLL_TIMEOUT_SECONDS", "25"))
		self.verbose_logging = os.getenv("VERBOSE_LOGGING", "true").strip().lower() in {
			"1",
			"true",
			"yes",
			"on",
		}
		self.auto_delete_webhook = os.getenv("AUTO_DELETE_WEBHOOK_ON_START", "true").strip().lower() in {
			"1",
			"true",
			"yes",
			"on",
		}
		self.notify_chat_id = os.getenv("APPROVAL_NOTIFY_CHAT_ID", "").strip()

		if not self.bot_token:
			raise RuntimeError("TELEGRAM_BOT_TOKEN is required.")
		if not self.mongo_uri:
			raise RuntimeError("MONGODB_URI is required.")

		self.allowed_channel_ids, self.allowed_usernames = parse_channel_filters(self.channels_value)

		self.base_api_url = f"https://api.telegram.org/bot{self.bot_token}"
		self.base_file_url = f"https://api.telegram.org/file/bot{self.bot_token}"

		self.mongo_client = MongoClient(self.mongo_uri)
		self.db = self.mongo_client[self.mongo_db_name]
		self.raw_posts: Collection = self.db["raw_posts"]
		self.pending_events: Collection = self.db["pending_events"]
		self.last_update_id: Optional[int] = None
		self.last_channel_post_fingerprint: Optional[str] = None
		self.poll_cycles = 0
		self.processed_updates = 0

		self.ensure_indexes()

	def log(self, level: str, message: str) -> None:
		now = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
		print(f"[{level}] {now} {message}")

	def log_verbose(self, message: str) -> None:
		if self.verbose_logging:
			self.log("DEBUG", message)

	def ensure_indexes(self) -> None:
		self.raw_posts.create_index("fingerprint", unique=True)
		self.pending_events.create_index("fingerprint", unique=True)
		self.pending_events.create_index("status")

	def is_channel_allowed(self, chat: Dict[str, Any]) -> bool:
		if not self.allowed_channel_ids and not self.allowed_usernames:
			return True

		chat_id = str(chat.get("id", ""))
		username = str(chat.get("username", "")).lower()

		if chat_id and chat_id in self.allowed_channel_ids:
			return True
		if username and username in self.allowed_usernames:
			return True

		return False

	def get_updates(self, offset: Optional[int]) -> Dict[str, Any]:
		payload: Dict[str, Any] = {
			"timeout": self.long_poll_timeout,
			"allowed_updates": ["channel_post", "edited_channel_post"],
		}
		if offset is not None:
			payload["offset"] = offset

		response = requests.get(
			f"{self.base_api_url}/getUpdates",
			params=payload,
			timeout=self.long_poll_timeout + 10,
		)
		response.raise_for_status()
		return response.json()

	def get_me(self) -> Optional[Dict[str, Any]]:
		try:
			response = requests.get(f"{self.base_api_url}/getMe", timeout=20)
			response.raise_for_status()
			payload = response.json()
			if not payload.get("ok"):
				self.log("WARN", f"getMe returned not ok: {json.dumps(payload)}")
				return None

			result = payload.get("result") or {}
			self.log(
				"INFO",
				f"Bot connected: id={result.get('id')} username=@{result.get('username')} name={result.get('first_name')}",
			)
			return result
		except requests.RequestException as error:
			self.log("ERROR", f"Failed to call getMe: {error}")
			return None

	def verify_channel_access(self) -> None:
		targets = sorted(self.allowed_channel_ids) + [f"@{name}" for name in sorted(self.allowed_usernames)]
		if not targets:
			self.log("WARN", "No channels configured. TELEGRAM_CHANNELS is empty.")
			return

		self.log("INFO", f"Verifying access to {len(targets)} configured channel target(s)")
		for target in targets:
			try:
				response = requests.get(
					f"{self.base_api_url}/getChat",
					params={"chat_id": target},
					timeout=20,
				)
				response.raise_for_status()
				payload = response.json()
				if payload.get("ok"):
					chat = payload.get("result") or {}
					self.log(
						"INFO",
						f"Channel access OK: target={target} resolved_id={chat.get('id')} title={chat.get('title')}",
					)
				else:
					self.log("WARN", f"Channel access failed for {target}: {json.dumps(payload)}")
			except requests.RequestException as error:
				self.log("WARN", f"Channel access request failed for {target}: {error}")

	def delete_webhook(self, drop_pending_updates: bool = False) -> bool:
		try:
			response = requests.get(
				f"{self.base_api_url}/deleteWebhook",
				params={"drop_pending_updates": str(drop_pending_updates).lower()},
				timeout=20,
			)
			response.raise_for_status()
			data = response.json()
			if not data.get("ok"):
				self.log("WARN", f"deleteWebhook returned not ok: {json.dumps(data)}")
				return False

			self.log("INFO", "Webhook cleared for polling mode")
			return True
		except requests.RequestException as error:
			self.log("WARN", f"Failed to delete webhook: {error}")
			return False

	def get_file_url(self, file_id: str) -> Optional[str]:
		response = requests.get(
			f"{self.base_api_url}/getFile",
			params={"file_id": file_id},
			timeout=20,
		)
		response.raise_for_status()
		data = response.json()

		if not data.get("ok"):
			return None

		file_path = data.get("result", {}).get("file_path")
		if not file_path:
			return None

		return f"{self.base_file_url}/{file_path}"

	def extract_image_url(self, message: Dict[str, Any]) -> Optional[str]:
		photos = message.get("photo") or []
		if not photos:
			return None

		largest = photos[-1]
		file_id = largest.get("file_id")
		if not file_id:
			return None

		try:
			self.log_verbose(f"Resolving image file URL for message_id={message.get('message_id')}")
			return self.get_file_url(file_id)
		except Exception as error:  # noqa: BLE001
			self.log("WARN", f"Failed to resolve Telegram file URL: {error}")
			return None

	def passes_keyword_filter(self, text: str) -> bool:
		normalized = text.lower()
		return any(keyword in normalized for keyword in self.keywords)

	def parse_strict_event_format(self, text: str, image_url: Optional[str]) -> Optional[Dict[str, Any]]:
		if not image_url:
			return None

		required_keys = {
			"title",
			"description",
			"overview",
			"venue",
			"location",
			"date",
			"time",
			"mode",
			"audience",
			"organizer",
			"tags",
			"agenda",
		}

		lines = [line.rstrip() for line in text.splitlines()]
		parsed: Dict[str, Any] = {}
		current_key: Optional[str] = None

		for raw_line in lines:
			line = raw_line.strip()
			if not line:
				continue

			match = re.match(r"^([A-Za-z]+)\s*:\s*(.*)$", line)
			if match:
				key = match.group(1).strip().lower()
				value = match.group(2).strip()

				if key not in required_keys:
					current_key = None
					continue

				if key == "agenda":
					parsed.setdefault("agenda", [])
					if value:
						parsed["agenda"].append(value)
					current_key = "agenda"
				else:
					parsed[key] = value
					current_key = key
				continue

			if current_key == "agenda" and (line.startswith("-") or line.startswith("*")):
				item = line[1:].strip()
				if item:
					parsed.setdefault("agenda", []).append(item)

		missing = [key for key in required_keys if key not in parsed or not parsed.get(key)]
		if missing:
			return None

		if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(parsed["date"])):
			return None

		if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$", str(parsed["time"])):
			return None

		mode = str(parsed["mode"]).strip().lower()
		if mode not in {"in-person", "online", "hybrid"}:
			return None

		tags = [tag.strip().lower() for tag in str(parsed["tags"]).split(",") if tag.strip()]
		agenda_items = [str(item).strip() for item in parsed.get("agenda", []) if str(item).strip()]

		if not tags or not agenda_items:
			return None

		parsed_event = {
			"title": str(parsed["title"]).strip(),
			"description": str(parsed["description"]).strip(),
			"overview": str(parsed["overview"]).strip(),
			"venue": str(parsed["venue"]).strip(),
			"location": str(parsed["location"]).strip(),
			"date": str(parsed["date"]).strip(),
			"time": str(parsed["time"]).strip(),
			"mode": mode,
			"audience": str(parsed["audience"]).strip(),
			"organizer": str(parsed["organizer"]).strip(),
			"tags": tags,
			"agenda": agenda_items,
			"image": image_url,
		}

		for value in parsed_event.values():
			if isinstance(value, str) and not value.strip():
				return None

		return parsed_event

	def extract_basic_fields(self, text: str) -> Tuple[str, str]:
		lines = [line.strip() for line in text.splitlines() if line.strip()]
		if lines:
			title = lines[0][:180]
			description = "\n".join(lines[:8]).strip()
		else:
			title = "Untitled Event Post"
			description = text.strip() or "No text content"

		return title, description

	def build_source(self, chat: Dict[str, Any]) -> str:
		title = str(chat.get("title", "")).strip()
		username = str(chat.get("username", "")).strip()

		if username:
			return f"@{username}"
		if title:
			return title

		return str(chat.get("id", "Unknown channel"))

	def insert_raw_post(
		self,
		message: Dict[str, Any],
		chat: Dict[str, Any],
		text: str,
		image_url: Optional[str],
	) -> Optional[Dict[str, Any]]:
		source_channel_id = str(chat.get("id", ""))
		telegram_message_id = int(message.get("message_id"))
		fingerprint = f"{source_channel_id}:{telegram_message_id}"

		raw_doc = {
			"fingerprint": fingerprint,
			"sourceChannelId": source_channel_id,
			"sourceChannelTitle": str(chat.get("title", "")).strip() or None,
			"sourceChannelUsername": str(chat.get("username", "")).strip() or None,
			"telegramMessageId": telegram_message_id,
			"messageDate": datetime.fromtimestamp(message.get("date"), tz=timezone.utc)
			if message.get("date")
			else None,
			"text": text,
			"imageUrl": image_url,
			"mediaType": "photo" if image_url else "none",
			"rawPayload": message,
			"createdAt": datetime.now(tz=timezone.utc),
			"updatedAt": datetime.now(tz=timezone.utc),
		}

		try:
			result = self.raw_posts.insert_one(raw_doc)
			raw_doc["_id"] = result.inserted_id
			self.log_verbose(
				f"raw_posts insert success fingerprint={fingerprint} mongo_id={result.inserted_id}"
			)
			return raw_doc
		except DuplicateKeyError:
			self.log("INFO", f"Duplicate raw post skipped: {fingerprint}")
			return None

	def insert_pending_event(
		self,
		raw_doc: Dict[str, Any],
		parsed_event: Dict[str, Any],
		source: str,
	) -> bool:
		pending_doc = {
			"rawPostId": raw_doc["_id"],
			"fingerprint": raw_doc["fingerprint"],
			"sourceChannelId": raw_doc["sourceChannelId"],
			"source": source,
			"telegramMessageId": raw_doc["telegramMessageId"],
			"title": parsed_event["title"],
			"description": parsed_event["description"],
			"image": raw_doc.get("imageUrl"),
			"eventDate": parsed_event["date"],
			"originalMessage": raw_doc.get("text", ""),
			"parsedEvent": parsed_event,
			"status": "pending",
			"createdAt": datetime.now(tz=timezone.utc),
			"updatedAt": datetime.now(tz=timezone.utc),
		}

		try:
			self.pending_events.insert_one(pending_doc)
			self.log_verbose(f"pending_events insert success fingerprint={raw_doc['fingerprint']}")
			return True
		except DuplicateKeyError:
			self.log("INFO", f"Duplicate pending event skipped: {raw_doc['fingerprint']}")
			return False

	def notify_pending_event(self, title: str, source: str, fingerprint: str) -> None:
		if not self.notify_chat_id:
			return

		text = (
			"New pending event needs review\n"
			f"Title: {title}\n"
			f"Source: {source}\n"
			f"Fingerprint: {fingerprint}\n"
			"Open /admin/pending in the Next.js app."
		)

		try:
			response = requests.post(
				f"{self.base_api_url}/sendMessage",
				json={"chat_id": self.notify_chat_id, "text": text},
				timeout=20,
			)
			response.raise_for_status()
			self.log_verbose(f"Approval notification sent to chat_id={self.notify_chat_id}")
		except Exception as error:  # noqa: BLE001
			self.log("WARN", f"Failed to send approval notification: {error}")

	def process_message(self, message: Dict[str, Any]) -> None:
		chat = message.get("chat") or {}
		if not chat:
			self.log_verbose("Skipping update without chat payload")
			return
		if chat.get("type") != "channel":
			self.log_verbose("Skipping non-channel update")
			return
		if not self.is_channel_allowed(chat):
			self.log_verbose(
				f"Skipping channel not in allowlist chat_id={chat.get('id')} username={chat.get('username')}"
			)
			return

		text = (message.get("text") or message.get("caption") or "").strip()
		if not text:
			self.log_verbose(f"Skipping channel message without text/caption message_id={message.get('message_id')}")
			return

		source_channel_id = str(chat.get("id", ""))
		telegram_message_id = int(message.get("message_id"))
		fingerprint = f"{source_channel_id}:{telegram_message_id}"
		self.last_channel_post_fingerprint = fingerprint
		self.log(
			"INFO",
			f"Channel message received fingerprint={fingerprint} title={chat.get('title')} username=@{chat.get('username')}",
		)

		image_url = self.extract_image_url(message)
		if image_url:
			self.log_verbose(f"Image detected on message fingerprint={fingerprint}")
		else:
			self.log_verbose(f"No image detected on message fingerprint={fingerprint}")

		raw_doc = self.insert_raw_post(message=message, chat=chat, text=text, image_url=image_url)
		if not raw_doc:
			return

		if not self.passes_keyword_filter(text):
			self.log("INFO", f"Keyword filter miss fingerprint={fingerprint}")
			return

		self.log("INFO", f"Keyword filter hit fingerprint={fingerprint}")
		parsed_event = self.parse_strict_event_format(text=text, image_url=image_url)
		if not parsed_event:
			self.log(
				"INFO",
				f"Strict format mismatch or missing image. Skipping pending event fingerprint={fingerprint}",
			)
			self.log_verbose(STRICT_MODE_FORMAT_HELP)
			return

		source = self.build_source(chat)
		inserted = self.insert_pending_event(raw_doc=raw_doc, parsed_event=parsed_event, source=source)

		if inserted:
			self.notify_pending_event(
				title=parsed_event["title"],
				source=source,
				fingerprint=raw_doc["fingerprint"],
			)
			self.log("INFO", f"Pending event created: {raw_doc['fingerprint']}")

	def run(self) -> None:
		self.log("INFO", "Telegram worker started")
		self.log(f"INFO", f"DB: {self.mongo_db_name}")
		self.log("INFO", f"Keywords: {', '.join(self.keywords)}")
		self.log(
			"INFO",
			f"Config: poll_interval={self.poll_interval}s long_poll_timeout={self.long_poll_timeout}s verbose={self.verbose_logging}",
		)
		if self.allowed_channel_ids or self.allowed_usernames:
			self.log(
				"INFO",
				"Channel filter enabled: "
				f"ids={sorted(self.allowed_channel_ids)} usernames={sorted(self.allowed_usernames)}"
			)
		else:
			self.log("WARN", "TELEGRAM_CHANNELS is empty. Worker will process any channel posts the bot receives.")

		self.get_me()
		self.verify_channel_access()

		if self.auto_delete_webhook:
			self.delete_webhook(drop_pending_updates=False)
		else:
			self.log("INFO", "AUTO_DELETE_WEBHOOK_ON_START is disabled")

		offset: Optional[int] = None

		while True:
			try:
				self.poll_cycles += 1
				self.log_verbose(
					f"Polling cycle={self.poll_cycles} offset={offset} last_update_id={self.last_update_id} "
					f"last_post={self.last_channel_post_fingerprint}"
				)
				payload = self.get_updates(offset)
				if not payload.get("ok"):
					self.log("WARN", f"getUpdates returned not ok: {json.dumps(payload)}")
					time.sleep(self.poll_interval)
					continue

				updates = payload.get("result", [])
				self.log_verbose(f"Polling cycle={self.poll_cycles} updates_count={len(updates)}")
				for update in updates:
					update_id = update.get("update_id")
					if update_id is not None:
						self.last_update_id = int(update_id)
						offset = int(update_id) + 1
						self.processed_updates += 1
						self.log_verbose(
							f"Update received update_id={update_id} next_offset={offset} processed_total={self.processed_updates}"
						)

					message = update.get("channel_post") or update.get("edited_channel_post")
					if message:
						self.process_message(message)
			except requests.RequestException as error:
				message = str(error)
				status_code = getattr(getattr(error, "response", None), "status_code", None)
				if status_code == 409:
					self.log("WARN", "Telegram returned 409 Conflict. Another polling/webhook consumer is active.")
					if self.auto_delete_webhook:
						self.delete_webhook(drop_pending_updates=False)
				elif "Read timed out" in message:
					self.log(
						"INFO",
						"Long poll timeout reached without new updates (this can be normal when channel is idle).",
					)
				else:
					self.log("ERROR", f"Telegram API request failed: {error}")
				time.sleep(self.poll_interval)
			except KeyboardInterrupt:
				self.log("INFO", "Worker stopped by user")
				break
			except Exception as error:  # noqa: BLE001
				self.log("ERROR", f"Unexpected worker error: {error}")
				time.sleep(self.poll_interval)


if __name__ == "__main__":
	worker = TelegramEventWorker()
	worker.run()
