import re
import uuid
import json
import logging
import asyncio
from typing import Dict, Any, Union, AsyncGenerator
from openai import OpenAI
from evoloai_brain.database.users import UserCollection
from evoloai_brain.config import Config
from evoloai_brain.schemas.error import ErrorResponse
from evoloai_brain.utils.serialization import serialize_response
from evoloai_brain.schemas.resumate import ResumateResponse, ResumateRequest
from evoloai_brain.services.resume_service import ResumeService
from partial_json_parser import loads as partial_json_loads
from datetime import datetime
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from deepmerge import always_merger

logger = logging.getLogger(__name__)

RESUMATE_OPENAI_ASSISTANT_ID_V2 = Config.RESUMATE_OPENAI_ASSISTANT_ID_V2

class ResumateAssistantServicev2:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.user_manager = UserCollection(user_id)
        self.openai_client = OpenAI(api_key=Config.OPENAI_API_KEY)
        self.pusher_client = None

    def partial_json(self, partial_json_str: str) -> str:  
        """Extract assistant_response from partial JSON"""
        try:
            parsed = partial_json_loads(partial_json_str)

            if isinstance(parsed, dict) and "assistant_response" in parsed:
                return parsed.get("assistant_response", "")

            return ""
        except Exception as e:
            return ""

    async def init_score_and_chat(self) -> StreamingResponse:
        """Initialize chat with streaming response following Documate pattern"""
        try:
            user_profile = await self.user_manager.get_current_user()
            if user_profile is None:
                raise HTTPException(status_code=404, detail="User profile not found")

            today = datetime.today().strftime("%B %d, %Y")

            user_message = (
                f"Hi Rose! I'm excited to build my resume with your help. "
                f"welcome me, and let's begin. "
                f"Today is {today}. Please calculate my initial ATS score based on the information below, "
                f"Here is my current profile: {serialize_response(user_profile)}"
            )

            # Remove any existing thread ID
            if "resumate_thread_id" in user_profile:
                del user_profile["resumate_thread_id"]

            # Create new thread
            try:
                thread = self.openai_client.beta.threads.create(
                    messages=[{"role": "user", "content": user_message}]
                )
                resumate_thread_id = thread.id
            except Exception as e:
                logger.error("Error creating OpenAI thread", exc_info=True, extra={"user_id": self.user_id})
                raise HTTPException(status_code=500, detail="Failed to initialize chat thread")

            # Update user profile with thread ID immediately
            user_profile["resumate_thread_id"] = resumate_thread_id
            await self.user_manager.set_user(user_profile)

            async def generate_stream() -> AsyncGenerator[bytes, None]:
                try:
                    # Send initial response with thread ID
                    initial_data = {
                        "resumate_thread_id": resumate_thread_id,
                        "type": "thread_created"
                    }
                    yield f"data: {json.dumps(initial_data)}\n\n".encode('utf-8')

                    full_response = ""
                    last_extracted_length = 0

                    # Create streaming run - we need to handle this in async context
                    # Since OpenAI SDK doesn't provide async streaming, we'll use asyncio.to_thread
                    def stream_run():
                        return self.openai_client.beta.threads.runs.stream(
                            thread_id=resumate_thread_id,
                            assistant_id=RESUMATE_OPENAI_ASSISTANT_ID_V2,
                        )
                    
                    # Run the streaming in a thread to avoid blocking
                    stream = await asyncio.to_thread(stream_run)
                    
                    with stream:
                        for event in stream:
                            # Allow other async operations to run
                            await asyncio.sleep(0)
                            
                            if event.event == 'thread.message.delta':
                                if hasattr(event.data, 'delta') and hasattr(event.data.delta, 'content'):
                                    for content in event.data.delta.content:
                                        if hasattr(content, 'text') and hasattr(content.text, 'value'):
                                            chunk_text = content.text.value
                                            full_response += chunk_text

                                            # Extract text from partial JSON if using JSON format
                                            current_text = self.partial_json(full_response)

                                            # Only send new characters that we haven't sent yet
                                            if len(current_text) > last_extracted_length:
                                                new_chunk = current_text[last_extracted_length:]
                                                last_extracted_length = len(current_text)

                                                # Stream word by word for better UX
                                                words = re.findall(r'\S+\s*', new_chunk)
                                                for word in words:
                                                    chunk_data = {
                                                        "type": "text",
                                                        "chunk": word
                                                    }
                                                    yield f"data: {json.dumps(chunk_data)}\n\n".encode('utf-8')
                                                    await asyncio.sleep(0)  # Allow other async operations

                            elif event.event == 'thread.run.completed':
                                # Process the complete response
                                assistant_response_dict = None
                                try:
                                    assistant_response_dict = partial_json_loads(full_response.strip())
                                    if not isinstance(assistant_response_dict, dict):
                                        raise ValueError("Invalid response format")
                                except (json.JSONDecodeError, TypeError, ValueError, Exception):
                                    assistant_response_dict = {
                                        "assistant_response": full_response.strip(),
                                        "ats_score": None,
                                        "update_profile": False,
                                        "generate_resume": False,
                                        "user_profile": {}
                                    }

                                # Process response data
                                assistant_response = assistant_response_dict.get("assistant_response", "")
                                new_ats_score = assistant_response_dict.get("ats_score", 0)

                                # Get latest user profile
                                user_profile = await self.user_manager.get_current_user()

                                if "ats_score" in user_profile and user_profile["ats_score"] is not None:
                                    ats_score = max(new_ats_score, user_profile["ats_score"])
                                else:
                                    ats_score = new_ats_score

                                user_data = assistant_response_dict.get("user_profile", {})
                                generate_resume = assistant_response_dict.get("generate_resume", False)

                                user_profile["ats_score"] = ats_score

                                # Update user profile
                                updated_user = await self._set_user_profile(user_profile, user_data, resumate_thread_id)

                                # Generate resume if needed
                                resume_url = None
                                if generate_resume:
                                    if self.pusher_client:
                                        # Run pusher trigger in background to avoid blocking
                                        await asyncio.to_thread(
                                            self.pusher_client.trigger,
                                            self.user_id, 
                                            "resume-event", 
                                            {
                                                "phase": "resumate_initiated",
                                                "user_id": self.user_id,
                                                "message": "Resume generation in progress..."
                                            }
                                        )
                                    resume_result = await self.generate_resume_tool()
                                    if isinstance(resume_result, dict) and "resume_url" in resume_result:
                                        resume_url = resume_result.get("resume_url")

                                # Send completion data
                                completion_data = {
                                    "type": "completed",
                                    "resumate_thread_id": resumate_thread_id,
                                    "ats_score": ats_score,
                                    "resume_url": resume_url
                                }
                                yield f"data: {json.dumps(completion_data)}\n\n".encode('utf-8')

                            elif event.event == 'thread.run.cancelled':
                                error_data = {
                                    "type": "error",
                                    "message": "Run was cancelled"
                                }
                                yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')
                                break

                            elif event.event == 'thread.run.failed':
                                error_data = {
                                    "type": "error",
                                    "message": "Run failed"
                                }
                                yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')
                                break

                except Exception as e:
                    logger.error("Error in streaming run", exc_info=True, extra={
                        "user_id": self.user_id, 
                        "thread_id": resumate_thread_id, 
                        "error": str(e)
                    })
                    error_data = {
                        "type": "error",
                        "message": f"Failed to process initial message: {str(e)}"
                    }
                    yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')

            return StreamingResponse(
                generate_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Error in init_score_and_chat", exc_info=True, extra={"user_id": self.user_id})
            raise HTTPException(status_code=500, detail="Internal server error")

    async def get_score_and_chat(self, resumate_thread_id: str, resumate_input: ResumateRequest) -> StreamingResponse:
        """Continue chat with streaming response following Documate pattern"""
        try:
            if not isinstance(resumate_input, ResumateRequest) or not resumate_thread_id or not resumate_input.message:
                raise HTTPException(status_code=400, detail="Both resumate_thread_id and message are required")

            user_profile = await self.user_manager.get_current_user()
            if user_profile is None:
                raise HTTPException(status_code=404, detail="User profile not found")

            user_message = f"{resumate_input.message}. Current profile: {serialize_response(user_profile)}"

            # Validate thread ID
            if "resumate_thread_id" in user_profile and user_profile["resumate_thread_id"]:
                if user_profile["resumate_thread_id"] != resumate_thread_id:
                    raise HTTPException(status_code=400, detail="ThreadId does not match the user profile's resumate_thread_id")
                del user_profile["resumate_thread_id"]

            # Create message in thread
            try:
                message = self.openai_client.beta.threads.messages.create(
                    thread_id=resumate_thread_id,
                    role="user",
                    content=user_message
                )
            except Exception as e:
                logger.error("Error creating message in thread", exc_info=True, extra={
                    "user_id": self.user_id, 
                    "thread_id": resumate_thread_id
                })
                raise HTTPException(status_code=500, detail="Failed to send message to chat thread")

            async def generate_stream() -> AsyncGenerator[bytes, None]:
                try:
                    full_response = ""
                    last_extracted_length = 0

                    # Create streaming run - handle in async context
                    def stream_run():
                        return self.openai_client.beta.threads.runs.stream(
                            thread_id=resumate_thread_id,
                            assistant_id=RESUMATE_OPENAI_ASSISTANT_ID_V2,
                        )
                    
                    # Run the streaming in a thread to avoid blocking
                    stream = await asyncio.to_thread(stream_run)
                    
                    with stream:
                        for event in stream:
                            # Allow other async operations to run
                            await asyncio.sleep(0)
                            
                            if event.event == 'thread.message.delta':
                                if hasattr(event.data, 'delta') and hasattr(event.data.delta, 'content'):
                                    for content in event.data.delta.content:
                                        if hasattr(content, 'text') and hasattr(content.text, 'value'):
                                            chunk_text = content.text.value
                                            full_response += chunk_text

                                            # Extract assistant_response from partial JSON
                                            current_text = self.partial_json(full_response)

                                            # Only send new characters that we haven't sent yet
                                            if len(current_text) > last_extracted_length:
                                                new_chunk = current_text[last_extracted_length:]
                                                last_extracted_length = len(current_text)

                                                # Split into words for smoother streaming
                                                words = re.findall(r'\S+\s*', new_chunk)
                                                for word in words:
                                                    chunk_data = {
                                                        "type": "text",
                                                        "chunk": word
                                                    }
                                                    yield f"data: {json.dumps(chunk_data)}\n\n".encode('utf-8')
                                                    await asyncio.sleep(0)  # Allow other async operations

                            elif event.event == 'thread.run.completed':
                                # Process the complete response
                                assistant_response_dict = None
                                try:
                                    assistant_response_dict = partial_json_loads(full_response.strip())
                                    if not isinstance(assistant_response_dict, dict):
                                        raise ValueError("Invalid response format")
                                except (json.JSONDecodeError, TypeError, ValueError, Exception):
                                    assistant_response_dict = {
                                        "assistant_response": full_response.strip(),
                                        "ats_score": None,
                                        "update_profile": False,
                                        "generate_resume": False,
                                        "user_profile": {}
                                    }

                                # Process response data
                                assistant_response = assistant_response_dict.get("assistant_response", "")
                                new_ats_score = assistant_response_dict.get("ats_score", 0)

                                # Get latest user profile
                                user_profile = await self.user_manager.get_current_user()

                                # Determine final ATS score (keep higher value)
                                if "ats_score" in user_profile and user_profile["ats_score"] is not None:
                                    ats_score = max(new_ats_score, user_profile["ats_score"])
                                else:
                                    ats_score = new_ats_score

                                user_data = assistant_response_dict.get("user_profile", {})
                                generate_resume = assistant_response_dict.get("generate_resume", False)

                                user_profile["ats_score"] = ats_score

                                # Update profile
                                updated_user = await self._set_user_profile(user_profile, user_data, resumate_thread_id)

                                # Generate resume if needed
                                resume_url = None
                                if generate_resume:
                                    resume_result = await self.generate_resume_tool()
                                    if isinstance(resume_result, dict) and "resume_url" in resume_result:
                                        resume_url = resume_result.get("resume_url")

                                # Send completion data
                                completion_data = {
                                    "type": "completed",
                                    "resumate_thread_id": resumate_thread_id,
                                    "ats_score": ats_score,
                                    "resume_url": resume_url
                                }
                                yield f"data: {json.dumps(completion_data)}\n\n".encode('utf-8')

                            elif event.event == 'thread.run.cancelled':
                                error_data = {
                                    "type": "error",
                                    "message": "Run was cancelled",
                                    "resumate_thread_id": resumate_thread_id
                                }
                                yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')
                                break

                            elif event.event == 'thread.run.failed':
                                error_data = {
                                    "type": "error",
                                    "message": "Run failed",
                                    "resumate_thread_id": resumate_thread_id
                                }
                                yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')
                                break

                except Exception as e:
                    logger.error("Error in streaming run", exc_info=True, extra={
                        "user_id": self.user_id, 
                        "thread_id": resumate_thread_id, 
                        "error": str(e)
                    })
                    error_data = {
                        "type": "error",
                        "message": f"Failed to process message: {str(e)}",
                        "resumate_thread_id": resumate_thread_id
                    }
                    yield f"data: {json.dumps(error_data)}\n\n".encode('utf-8')

            return StreamingResponse(
                generate_stream(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Error in get_score_and_chat", exc_info=True, extra={
                "user_id": self.user_id, 
                "thread_id": resumate_thread_id
            })
            raise HTTPException(status_code=500, detail="Internal server error")

    def stop_incomplete_runs(self, user_id: str, resumate_thread_id: str) -> Dict[str, Any] | ErrorResponse:
        """Stop incomplete runs - same as Documate pattern"""
        try:
            if resumate_thread_id is None or resumate_thread_id.__len__() == 0:
                raise HTTPException(status_code=400, detail="Field resumate_thread_id is required")

            runs = self.openai_client.beta.threads.runs.list(thread_id=resumate_thread_id)
            for run in runs.data:
                if run.status in ["in_progress", "queued"]:
                    self.openai_client.beta.threads.runs.cancel(
                        thread_id=resumate_thread_id, 
                        run_id=run.id
                    )
                    logger.info(f"Stopped incomplete run with ID: {run.id} in thread: {resumate_thread_id}")

            return {"success": True}

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Error stopping incomplete runs", exc_info=True, extra={
                "user_id": user_id, 
                "thread_id": resumate_thread_id
            })
            raise HTTPException(status_code=500, detail="Failed to stop incomplete runs")

    async def _set_user_profile(
        self,
        user_profile: Dict[str, Any],
        new_user_profile: Dict[str, Any],
        resumate_thread_id: str
    ) -> Dict[str, Any]:
        """Set user profile with merged data"""
        try:
            merged_profile = dict(user_profile)
            merged_profile = always_merger.merge(merged_profile, new_user_profile)
            merged_profile["resumate_thread_id"] = resumate_thread_id

            await self.user_manager.set_user(merged_profile)
            return merged_profile

        except Exception as e:
            logger.error("Error setting user profile", exc_info=True, extra={"user_id": self.user_id})
            raise e

    async def generate_resume_tool(self) -> Dict[str, Any] | ErrorResponse:
        """Generate resume using resume service"""
        try:
            resume_service = ResumeService(self.user_id)
            result = await resume_service.generate_resume()

            if self.pusher_client:
                # Run pusher trigger in background to avoid blocking
                await asyncio.to_thread(
                    self.pusher_client.trigger,
                    self.user_id, 
                    "resume-event", 
                    {
                        "phase": "resume_generated",
                        "user_id": self.user_id,
                        "message": "Completed successfully",
                        "resume_url": result.get("resume_url") if result else None
                    }
                )

            if isinstance(result, ErrorResponse):
                logger.error("Resume generation failed", extra={
                    "user_id": self.user_id, 
                    "error": result.message
                })
                return result

            return result

        except Exception as e:
            logger.error("Error in resume generation tool", exc_info=True, extra={"user_id": self.user_id})
            raise HTTPException(status_code=500, detail="Failed to generate resume")